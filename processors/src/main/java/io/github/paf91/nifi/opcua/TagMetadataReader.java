/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.eclipse.milo.opcua.stack.core.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.structured.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/** Optional startup enrichment, bounded by one request-timeout budget for the whole snapshot. */
final class TagMetadataReader {
    // OPC 10000-3, 8.2.4 limits string identifiers in characters and opaque identifiers in bytes.
    private static final int MAX_IDENTIFIER_LENGTH = 4096;

    interface Decoder {
        Object decode(ExtensionObject value) throws Exception;
    }

    private final ConnectionAttempt attempt;
    private final long deadline;
    private final Function<List<ReadValueId>, CompletableFuture<ReadResponse>> read;
    private final Function<
                    List<BrowsePath>, CompletableFuture<TranslateBrowsePathsToNodeIdsResponse>>
            translate;
    private final NamespaceTable namespaces;
    private final Decoder decoder;

    TagMetadataReader(
            ConnectionAttempt attempt,
            int timeout,
            Function<List<ReadValueId>, CompletableFuture<ReadResponse>> read,
            Function<List<BrowsePath>, CompletableFuture<TranslateBrowsePathsToNodeIdsResponse>>
                    translate,
            NamespaceTable namespaces,
            Decoder decoder) {
        this.attempt = attempt;
        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        this.read = read;
        this.translate = translate;
        this.namespaces = namespaces;
        this.decoder = decoder;
    }

    List<TagDefinition> load(
            List<TagDefinition> tags, OutputSettings output, ConnectionDiagnostics diagnostics)
            throws Exception {
        if (!output.metadata()) return tags;
        diagnostics.stage("Reading optional tag metadata");
        String[] types = new String[tags.size()];
        TagMetadata[] units = new TagMetadata[tags.size()];
        Arrays.fill(units, TagMetadata.EMPTY);
        String failure = null;
        try {
            if (output.includes(OutputSettings.Field.dataType)) {
                List<ReadValueId> requests = new ArrayList<>();
                for (TagDefinition tag : tags)
                    requests.add(attribute(tag.nodeId(), AttributeId.DataType));
                List<String> results =
                        chunks(
                                requests,
                                read,
                                ReadResponse::getResults,
                                result -> {
                                    Object value = goodValue(result);
                                    return value instanceof NodeId
                                            ? typeText((NodeId) value)
                                            : null;
                                });
                for (int i = 0; i < results.size(); i++) types[i] = results.get(i);
            }
            if (output.includes(OutputSettings.Field.engineeringUnits)) {
                List<BrowsePath> paths = new ArrayList<>();
                for (TagDefinition tag : tags)
                    paths.add(
                            new BrowsePath(
                                    tag.nodeId(),
                                    new RelativePath(
                                            new RelativePathElement[] {
                                                new RelativePathElement(
                                                        Identifiers.HasProperty,
                                                        false,
                                                        true,
                                                        new QualifiedName(0, "EngineeringUnits"))
                                            })));
                List<NodeId> resolved =
                        chunks(
                                paths,
                                translate,
                                TranslateBrowsePathsToNodeIdsResponse::getResults,
                                result -> {
                                    if (result == null
                                            || result.getStatusCode() == null
                                            || !result.getStatusCode().isGood()
                                            || result.getTargets() == null
                                            || result.getTargets().length != 1) return null;
                                    BrowsePathTarget target = result.getTargets()[0];
                                    if (target == null
                                            || target.getRemainingPathIndex() == null
                                            || target.getRemainingPathIndex().longValue()
                                                    != 0xffffffffL
                                            || target.getTargetId() == null
                                            || !target.getTargetId().isLocal()) return null;
                                    NodeId id =
                                            target.getTargetId().toNodeId(namespaces).orElse(null);
                                    return id != null && fitsIdentifierLimit(id) ? id : null;
                                });
                List<ReadValueId> requests = new ArrayList<>();
                List<Integer> indexes = new ArrayList<>();
                for (int i = 0; i < resolved.size(); i++) {
                    NodeId id = resolved.get(i);
                    if (id != null) {
                        requests.add(attribute(id, AttributeId.Value));
                        indexes.add(i);
                    }
                }
                List<TagMetadata> values =
                        chunks(requests, read, ReadResponse::getResults, this::units);
                for (int i = 0; i < values.size(); i++) units[indexes.get(i)] = values.get(i);
            }
        } catch (CancellationException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            attempt.check();
            // Metadata is optional. Never block value subscription because a server omits it.
            String exceptionType = e.getClass().getSimpleName();
            failure =
                    UaException.extractStatusCode(e)
                            .flatMap(status -> StatusCodes.lookup(status.getValue()))
                            .map(details -> exceptionType + "(" + details[0] + ")")
                            .orElse(exceptionType);
        }
        List<TagDefinition> result = new ArrayList<>();
        int missingTypes = 0, missingUnits = 0;
        for (int i = 0; i < tags.size(); i++) {
            TagMetadata metadata = new TagMetadata(types[i], units[i]);
            if (output.includes(OutputSettings.Field.dataType) && metadata.dataType == null)
                missingTypes++;
            if (output.includes(OutputSettings.Field.engineeringUnits) && !metadata.hasUnits())
                missingUnits++;
            result.add(tags.get(i).withMetadata(metadata));
        }
        diagnostics.tagMetadata(tags.size(), missingTypes, missingUnits, failure);
        return result;
    }

    private static ReadValueId attribute(NodeId id, AttributeId attribute) {
        return new ReadValueId(id, attribute.uid(), null, QualifiedName.NULL_VALUE);
    }

    private static Object goodValue(DataValue value) {
        return value != null
                        && value.getStatusCode() != null
                        && value.getStatusCode().isGood()
                        && value.getValue() != null
                ? value.getValue().getValue()
                : null;
    }

    private static String typeText(NodeId id) {
        try {
            NotificationJson.checkNodeIdSize(id, TagMetadata.MAX_TEXT);
            String text = id.toParseableString();
            return text.length() <= TagMetadata.MAX_TEXT ? text : null;
        } catch (NotificationJson.RecordTooLargeException e) {
            return null;
        }
    }

    private static boolean fitsIdentifierLimit(NodeId id) {
        Object identifier = id.getIdentifier();
        if (identifier instanceof String) {
            String text = (String) identifier;
            return text.length() <= 2 * MAX_IDENTIFIER_LENGTH
                    && text.codePointCount(0, text.length()) <= MAX_IDENTIFIER_LENGTH;
        }
        return !(identifier instanceof ByteString)
                || ((ByteString) identifier).length() <= MAX_IDENTIFIER_LENGTH;
    }

    private TagMetadata units(DataValue result) {
        Object value = goodValue(result);
        if (value instanceof ExtensionObject) {
            ExtensionObject extension = (ExtensionObject) value;
            NodeId encoding = OpcValue.encodingId(extension);
            if (!Identifiers.EUInformation_Encoding_DefaultBinary.equals(encoding)
                    && !Identifiers.EUInformation_Encoding_DefaultXml.equals(encoding)
                    && !(OpcValue.jsonEncoded(extension)
                            && Identifiers.EUInformation.equals(encoding)))
                return TagMetadata.EMPTY;
            Object body = extension.getBody();
            if (body instanceof ByteString && ((ByteString) body).length() > 16384)
                return TagMetadata.EMPTY;
            if (body instanceof XmlElement
                    && ((XmlElement) body).getFragment() != null
                    && ((XmlElement) body).getFragment().length() > 16384) return TagMetadata.EMPTY;
            if (body instanceof String && ((String) body).length() > 16384)
                return TagMetadata.EMPTY;
            try {
                value = decoder.decode(extension);
            } catch (Exception e) {
                return TagMetadata.EMPTY;
            }
        }
        return value instanceof EUInformation
                ? new TagMetadata(null, (EUInformation) value)
                : TagMetadata.EMPTY;
    }

    private <Q, R, T, U> List<U> chunks(
            List<Q> requests,
            Function<List<Q>, CompletableFuture<R>> operation,
            Function<R, T[]> results,
            Function<T, U> normalize)
            throws Exception {
        List<U> collected = new ArrayList<>();
        int size = 100;
        for (int offset = 0; offset < requests.size(); ) {
            attempt.check();
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Optional metadata deadline expired");
            List<Q> chunk =
                    new ArrayList<>(
                            requests.subList(offset, Math.min(offset + size, requests.size())));
            try {
                R response =
                        attempt.await(
                                attempt.callIfActive(() -> operation.apply(chunk)),
                                Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                T[] values = results.apply(response);
                if (values == null || values.length != chunk.size())
                    throw new IllegalArgumentException("Incomplete tag metadata response");
                for (T value : values) {
                    attempt.check();
                    if (deadline - System.nanoTime() <= 0)
                        throw new TimeoutException("Optional metadata deadline expired");
                    collected.add(normalize.apply(value));
                }
                offset += chunk.size();
            } catch (Exception e) {
                StatusCode code = UaException.extractStatusCode(e).orElse(null);
                if (chunk.size() == 1 || code == null) throw e;
                long status = code.getValue();
                if (status != StatusCodes.Bad_TooManyOperations
                        && status != StatusCodes.Bad_ResponseTooLarge
                        && status != StatusCodes.Bad_RequestTooLarge) throw e;
                size = chunk.size() / 2;
            }
        }
        return collected;
    }
}
