/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.server.nodes.UaNodeContext;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaSerializationException;
import org.eclipse.milo.opcua.stack.core.channel.EncodingLimits;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadResponse;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Timeout(45)
class MetadataMessageLimitTest {
    @TempDir Path dir;
    private final SecureSubscriptionTest fixture = new SecureSubscriptionTest();

    @BeforeEach
    void start() throws Exception {
        fixture.dir = dir;
        fixture.encodingLimits = new EncodingLimits(16384, 2, 32768, 64);
        fixture.startServer();
    }

    @AfterEach
    void stop() throws Exception {
        fixture.closeServer();
    }

    @Test
    void readsAllDataTypesAfterTheClientRejectsAnOversizedBatch() throws Exception {
        // Add each variable to Milo's existing server namespace.
        UaNodeContext context =
                fixture.server
                        .getAddressSpaceManager()
                        .getManagedNode(new NodeId(1, "VendorServerInfo/AvailableProcessors"))
                        .get()
                        .getNodeContext();
        String prefix = String.join("", Collections.nCopies(50, "temperature."));
        List<TagDefinition> tags = new ArrayList<>();
        List<String> expectedTypes = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String name = "sensor" + i;
            NodeId id = new NodeId(1, prefix + name);
            NodeId type = i < 50 ? Identifiers.Double : Identifiers.Float;
            UaVariableNode.build(
                    context,
                    builder ->
                            builder.setNodeId(id)
                                    .setBrowseName(new QualifiedName(1, name))
                                    .setDisplayName(LocalizedText.english(name))
                                    .setDataType(type)
                                    .buildAndAdd());
            tags.add(new TagDefinition(name, id, 100, 10, true));
            expectedTypes.add(type.toParseableString());
        }

        OpcUaClient client = OpcUaClient.create(fixture.endpoint);
        try {
            client.connect().get(10, TimeUnit.SECONDS);
            List<Integer> sizes = new ArrayList<>();
            List<CompletableFuture<ReadResponse>> reads = new ArrayList<>();
            TagMetadataReader reader =
                    new TagMetadataReader(
                            new ConnectionAttempt(),
                            5000,
                            requests -> {
                                sizes.add(requests.size());
                                CompletableFuture<ReadResponse> read =
                                        client.read(0.0, TimestampsToReturn.Neither, requests);
                                reads.add(read);
                                return read;
                            },
                            client::translateBrowsePaths,
                            client.getNamespaceTable(),
                            extension -> extension.decode(client.getStaticSerializationContext()));
            ConnectionDiagnostics report = new ConnectionDiagnostics();
            List<TagDefinition> resolved =
                    reader.load(
                            tags,
                            OutputOptionsTest.output(
                                    "tag,value,dataType", "JSON Array", "ISO-8601", false, 0),
                            report);

            ExecutionException rejected =
                    assertThrows(
                            ExecutionException.class, () -> reads.get(0).get(1, TimeUnit.SECONDS));
            assertEquals(
                    StatusCodes.Bad_RequestTooLarge,
                    assertInstanceOf(UaSerializationException.class, rejected.getCause())
                            .getStatusCode()
                            .getValue());
            assertEquals(Arrays.asList(100, 50, 50), sizes);
            Map<?, ?> metadata = (Map<?, ?>) report.snapshot().get("tagMetadata");
            assertNull(metadata.get("failure"));
            assertEquals(0, metadata.get("missingDataTypes"));
            assertEquals(
                    expectedTypes,
                    resolved.stream()
                            .map(tag -> tag.metadata().dataType)
                            .collect(Collectors.toList()));
        } finally {
            client.disconnect().get(10, TimeUnit.SECONDS);
        }
    }
}
