/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.*;

import org.eclipse.milo.opcua.stack.core.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.structured.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;

@Timeout(15)
class TagMetadataTest {
    private static final OutputSettings OUTPUT =
            OutputOptionsTest.output(
                    "tag,value,dataType,engineeringUnits", "JSON Array", "ISO-8601", false, 0);
    private static final EUInformation UNITS =
            new EUInformation(
                    "http://www.opcfoundation.org/UA/units/un/cefact",
                    4408652,
                    LocalizedText.english("°C"),
                    LocalizedText.english("degree Celsius"));

    private static List<TagDefinition> tags(int count) {
        List<TagDefinition> tags = new ArrayList<>();
        for (int i = 0; i < count; i++)
            tags.add(TagDefinition.parse("tag" + i, "ns=2;i=" + (i + 1), 100, 10));
        return tags;
    }

    private static CompletableFuture<ReadResponse> read(List<ReadValueId> requests) {
        DataValue[] values = new DataValue[requests.size()];
        for (int i = 0; i < values.length; i++)
            values[i] =
                    new DataValue(
                            new Variant(
                                    requests.get(i)
                                                    .getAttributeId()
                                                    .equals(AttributeId.DataType.uid())
                                            ? Identifiers.Double
                                            : UNITS));
        return CompletableFuture.completedFuture(new ReadResponse(null, values, null));
    }

    private static CompletableFuture<TranslateBrowsePathsToNodeIdsResponse> translate(
            List<BrowsePath> paths) {
        BrowsePathResult[] results = new BrowsePathResult[paths.size()];
        for (int i = 0; i < results.length; i++) {
            assertEquals(
                    "EngineeringUnits",
                    paths.get(i).getRelativePath().getElements()[0].getTargetName().getName());
            assertEquals(
                    Identifiers.HasProperty,
                    paths.get(i).getRelativePath().getElements()[0].getReferenceTypeId());
            results[i] =
                    new BrowsePathResult(
                            StatusCode.GOOD,
                            new BrowsePathTarget[] {
                                new BrowsePathTarget(
                                        new NodeId(2, "units").expanded(), uint(0xffffffffL))
                            });
        }
        return CompletableFuture.completedFuture(
                new TranslateBrowsePathsToNodeIdsResponse(null, results, null));
    }

    private static TagMetadataReader reader(
            ConnectionAttempt attempt,
            int timeout,
            Function<List<ReadValueId>, CompletableFuture<ReadResponse>> read,
            Function<List<BrowsePath>, CompletableFuture<TranslateBrowsePathsToNodeIdsResponse>>
                    translate) {
        return new TagMetadataReader(
                attempt, timeout, read, translate, new NamespaceTable(), extension -> UNITS);
    }

    private static <T> CompletableFuture<T> failure(long status) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(new UaException(status));
        return future;
    }

    @Test
    void defaultConfigurationMakesNoMetadataRequests() throws Exception {
        List<TagDefinition> tags = tags(1);
        assertSame(
                tags,
                reader(
                                new ConnectionAttempt(),
                                1000,
                                requests -> {
                                    fail("Unrequested metadata read");
                                    return null;
                                },
                                paths -> {
                                    fail("Unrequested metadata browse");
                                    return null;
                                })
                        .load(tags, OutputSettings.DEFAULT, new ConnectionDiagnostics()));
    }

    @Test
    void batchesMetadataAndReusesTheBoundedSnapshotWithoutFurtherReads() throws Exception {
        AtomicInteger reads = new AtomicInteger(), browses = new AtomicInteger();
        Set<Integer> missingUnits = new HashSet<>(Arrays.asList(98, 101, 198, 201));
        Set<NodeId> unitTargets = new HashSet<>();
        ConnectionDiagnostics report = new ConnectionDiagnostics();
        List<TagDefinition> resolved =
                reader(
                                new ConnectionAttempt(),
                                3000,
                                requests -> {
                                    assertTrue(requests.size() <= 100);
                                    reads.incrementAndGet();
                                    DataValue[] values = new DataValue[requests.size()];
                                    for (int i = 0; i < requests.size(); i++) {
                                        ReadValueId request = requests.get(i);
                                        Object value;
                                        if (request.getAttributeId()
                                                .equals(AttributeId.DataType.uid())) {
                                            int tag =
                                                    ((Number) request.getNodeId().getIdentifier())
                                                                    .intValue()
                                                            - 1;
                                            value = new NodeId(3, 1000 + tag);
                                        } else {
                                            assertEquals(
                                                    AttributeId.Value.uid(),
                                                    request.getAttributeId());
                                            assertTrue(
                                                    unitTargets.add(request.getNodeId()),
                                                    "Each units target is read once");
                                            int tag =
                                                    Integer.parseInt(
                                                            request.getNodeId()
                                                                    .getIdentifier()
                                                                    .toString()
                                                                    .substring(6));
                                            assertFalse(missingUnits.contains(tag));
                                            value =
                                                    new EUInformation(
                                                            "urn:test:units:" + tag,
                                                            10000 + tag,
                                                            LocalizedText.english("unit-" + tag),
                                                            LocalizedText.english(
                                                                    "description-" + tag));
                                        }
                                        values[i] = new DataValue(new Variant(value));
                                    }
                                    return CompletableFuture.completedFuture(
                                            new ReadResponse(null, values, null));
                                },
                                paths -> {
                                    assertTrue(paths.size() <= 100);
                                    browses.incrementAndGet();
                                    BrowsePathResult[] results = new BrowsePathResult[paths.size()];
                                    for (int i = 0; i < paths.size(); i++) {
                                        BrowsePath path = paths.get(i);
                                        int tag =
                                                ((Number) path.getStartingNode().getIdentifier())
                                                                .intValue()
                                                        - 1;
                                        assertEquals(
                                                "EngineeringUnits",
                                                path.getRelativePath()
                                                        .getElements()[0]
                                                        .getTargetName()
                                                        .getName());
                                        results[i] =
                                                missingUnits.contains(tag)
                                                        ? new BrowsePathResult(
                                                                new StatusCode(
                                                                        StatusCodes.Bad_NoMatch),
                                                                null)
                                                        : new BrowsePathResult(
                                                                StatusCode.GOOD,
                                                                new BrowsePathTarget[] {
                                                                    new BrowsePathTarget(
                                                                            new NodeId(
                                                                                            2,
                                                                                            "units-"
                                                                                                    + tag)
                                                                                    .expanded(),
                                                                            uint(0xffffffffL))
                                                                });
                                    }
                                    return CompletableFuture.completedFuture(
                                            new TranslateBrowsePathsToNodeIdsResponse(
                                                    null, results, null));
                                })
                        .load(tags(205), OUTPUT, report);
        assertEquals(205, resolved.size());
        assertEquals(6, reads.get());
        assertEquals(3, browses.get());
        assertEquals(201, unitTargets.size());
        ObjectMapper json = new ObjectMapper();
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0; i < resolved.size(); i++) {
                TagDefinition tag = resolved.get(i);
                JsonNode record =
                        json.readTree(
                                NotificationJson.encode(
                                        tag, new DataValue(new Variant(2500 + i)), 4096, OUTPUT));
                assertEquals("tag" + i, record.get("tag").asText());
                assertEquals(2500 + i, record.get("value").intValue());
                assertEquals("ns=3;i=" + (1000 + i), record.get("dataType").asText());
                JsonNode units = record.get("engineeringUnits");
                if (missingUnits.contains(i))
                    assertTrue(units.isNull(), "Missing units for tag " + i);
                else {
                    assertEquals("urn:test:units:" + i, units.get("namespaceUri").asText());
                    assertEquals("unit-" + i, units.get("displayName").asText());
                    assertEquals("description-" + i, units.get("description").asText());
                    assertEquals(10000 + i, units.get("unitId").intValue());
                }
            }
        }
        assertEquals(6, reads.get());
        assertEquals(3, browses.get());
        assertNull(((Map<?, ?>) report.snapshot().get("tagMetadata")).get("failure"));
        assertSame(
                TagMetadata.EMPTY,
                resolved.get(0).resolved(new NodeId(4, 1)).metadata(),
                "New namespace/session resolution must discard cached metadata");
    }

    @ParameterizedTest
    @ValueSource(strings = {"dataType", "engineeringUnits"})
    void readsOnlySelectedMetadata(String field) throws Exception {
        AtomicInteger reads = new AtomicInteger(), browses = new AtomicInteger();
        OutputSettings selected =
                OutputOptionsTest.output("tag,value," + field, "JSON Array", "ISO-8601", false, 0);
        reader(
                        new ConnectionAttempt(),
                        2000,
                        requests -> {
                            reads.incrementAndGet();
                            assertEquals(
                                    field.equals("dataType")
                                            ? AttributeId.DataType.uid()
                                            : AttributeId.Value.uid(),
                                    requests.get(0).getAttributeId());
                            return read(requests);
                        },
                        paths -> {
                            browses.incrementAndGet();
                            return translate(paths);
                        })
                .load(tags(2), selected, new ConnectionDiagnostics());
        assertEquals(1, reads.get());
        assertEquals(field.equals("dataType") ? 0 : 1, browses.get());
    }

    @Test
    void adaptsToUnadvertisedServerOperationLimits() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<TagDefinition> resolved =
                reader(
                                new ConnectionAttempt(),
                                2000,
                                requests -> {
                                    attempts.incrementAndGet();
                                    return requests.size() > 2
                                            ? failure(StatusCodes.Bad_TooManyOperations)
                                            : read(requests);
                                },
                                paths ->
                                        paths.size() > 2
                                                ? failure(StatusCodes.Bad_TooManyOperations)
                                                : translate(paths))
                        .load(tags(5), OUTPUT, new ConnectionDiagnostics());
        assertTrue(attempts.get() > 4);
        for (TagDefinition tag : resolved) {
            assertNotNull(tag.metadata().dataType);
            assertTrue(tag.metadata().hasUnits());
        }
    }

    @Test
    void missingPropertiesAndBadAttributesRemainNullWithoutFailingSubscriptionSetup()
            throws Exception {
        List<TagDefinition> resolved =
                reader(
                                new ConnectionAttempt(),
                                2000,
                                requests ->
                                        CompletableFuture.completedFuture(
                                                new ReadResponse(
                                                        null,
                                                        new DataValue[] {
                                                            new DataValue(StatusCode.BAD)
                                                        },
                                                        null)),
                                paths ->
                                        CompletableFuture.completedFuture(
                                                new TranslateBrowsePathsToNodeIdsResponse(
                                                        null,
                                                        new BrowsePathResult[] {
                                                            new BrowsePathResult(
                                                                    new StatusCode(
                                                                            StatusCodes
                                                                                    .Bad_NoMatch),
                                                                    null)
                                                        },
                                                        null)))
                        .load(tags(1), OUTPUT, new ConnectionDiagnostics());
        assertNull(resolved.get(0).metadata().dataType);
        assertFalse(resolved.get(0).metadata().hasUnits());
    }

    @ParameterizedTest
    @ValueSource(strings = {"multiple", "partial", "remote", "missing"})
    void rejectsAmbiguousPartialOrRemotePropertyTargets(String scenario) throws Exception {
        ExpandedNodeId id =
                scenario.equals("remote")
                        ? ExpandedNodeId.parse("svr=1;ns=2;i=1")
                        : new NodeId(2, 1).expanded();
        BrowsePathTarget target =
                new BrowsePathTarget(id, uint(scenario.equals("partial") ? 0 : 0xffffffffL));
        BrowsePathTarget[] targets =
                scenario.equals("multiple")
                        ? new BrowsePathTarget[] {target, target}
                        : scenario.equals("missing")
                                ? new BrowsePathTarget[0]
                                : new BrowsePathTarget[] {target};
        OutputSettings selected =
                OutputOptionsTest.output(
                        "tag,value,engineeringUnits", "JSON Array", "ISO-8601", false, 0);
        List<TagDefinition> resolved =
                reader(
                                new ConnectionAttempt(),
                                2000,
                                requests -> {
                                    fail("Must not read an invalid target");
                                    return null;
                                },
                                paths ->
                                        CompletableFuture.completedFuture(
                                                new TranslateBrowsePathsToNodeIdsResponse(
                                                        null,
                                                        new BrowsePathResult[] {
                                                            new BrowsePathResult(
                                                                    StatusCode.GOOD, targets)
                                                        },
                                                        null)))
                        .load(tags(1), selected, new ConnectionDiagnostics());
        assertFalse(resolved.get(0).metadata().hasUnits());
    }

    @Test
    void oneDeadlineCancelsStalledMetadataAndFallsBackToNulls() throws Exception {
        CompletableFuture<ReadResponse> pending = new CompletableFuture<>();
        ConnectionDiagnostics report = new ConnectionDiagnostics();
        long started = System.nanoTime();
        List<TagDefinition> resolved =
                reader(new ConnectionAttempt(), 80, requests -> pending, TagMetadataTest::translate)
                        .load(tags(200), OUTPUT, report);
        assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
        assertTrue(pending.isCancelled());
        assertEquals(
                "TimeoutException",
                ((Map<?, ?>) report.snapshot().get("tagMetadata")).get("failure"));
        assertEquals(200, resolved.size());
        assertNull(resolved.get(0).metadata().dataType);
    }

    @Test
    void stopCancellationIsNotConvertedIntoOptionalMetadataFailure() throws Exception {
        ConnectionAttempt attempt = new ConnectionAttempt();
        AtomicInteger calls = new AtomicInteger();
        assertThrows(
                CancellationException.class,
                () ->
                        reader(
                                        attempt,
                                        2000,
                                        requests -> {
                                            calls.incrementAndGet();
                                            attempt.cancel();
                                            return new CompletableFuture<>();
                                        },
                                        TagMetadataTest::translate)
                                .load(tags(1), OUTPUT, new ConnectionDiagnostics()));
        assertEquals(1, calls.get());
    }

    @Test
    void oversizedOrMalformedMetadataDoesNotEnterTheCache() throws Exception {
        String huge = String.join("", Collections.nCopies(2000, "x"));
        TagMetadata metadata =
                new TagMetadata(
                        huge,
                        new EUInformation(
                                huge, 1, LocalizedText.english("x"), LocalizedText.english("x")));
        assertNull(metadata.dataType);
        assertFalse(metadata.hasUnits());
        ConnectionDiagnostics report = new ConnectionDiagnostics();
        List<TagDefinition> resolved =
                reader(
                                new ConnectionAttempt(),
                                2000,
                                requests ->
                                        CompletableFuture.completedFuture(
                                                new ReadResponse(null, new DataValue[0], null)),
                                TagMetadataTest::translate)
                        .load(tags(1), OUTPUT, report);
        assertNull(resolved.get(0).metadata().dataType);
        assertEquals(
                "IllegalArgumentException",
                ((Map<?, ?>) report.snapshot().get("tagMetadata")).get("failure"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "xml", "unknown"})
    void refusesOversizedOrUnrelatedExtensionBodiesBeforeDecoding(String kind) throws Exception {
        NodeId id =
                kind.equals("binary")
                        ? Identifiers.EUInformation_Encoding_DefaultBinary
                        : kind.equals("xml")
                                ? Identifiers.EUInformation_Encoding_DefaultXml
                                : new NodeId(2, 999);
        Object body =
                kind.equals("xml")
                        ? MemoryBoundsTest.xml(String.join("", Collections.nCopies(16385, "x")))
                        : new ByteString(new byte[kind.equals("binary") ? 16385 : 4]);
        Class<?> type = kind.equals("xml") ? XmlElement.class : ByteString.class;
        ExtensionObject extension;
        try {
            extension =
                    ExtensionObject.class.getConstructor(type, NodeId.class).newInstance(body, id);
        } catch (NoSuchMethodException modern) {
            extension =
                    (ExtensionObject)
                            ExtensionObject.class
                                    .getMethod("of", type, NodeId.class)
                                    .invoke(null, body, id);
        }
        assertRejectedExtension(extension);
    }

    static void assertRejectedExtension(ExtensionObject encoded) throws Exception {
        OutputSettings selected =
                OutputOptionsTest.output(
                        "tag,value,engineeringUnits", "JSON Array", "ISO-8601", false, 0);
        TagMetadataReader reader =
                new TagMetadataReader(
                        new ConnectionAttempt(),
                        2000,
                        requests ->
                                CompletableFuture.completedFuture(
                                        new ReadResponse(
                                                null,
                                                new DataValue[] {
                                                    new DataValue(new Variant(encoded))
                                                },
                                                null)),
                        TagMetadataTest::translate,
                        new NamespaceTable(),
                        ignored -> {
                            fail("Unsafe extension must not be decoded");
                            return null;
                        });
        assertFalse(
                reader.load(tags(1), selected, new ConnectionDiagnostics())
                        .get(0)
                        .metadata()
                        .hasUnits());
    }
}
