/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.*;

class InitialNotificationTest {
    @TempDir Path dir;
    private final SecureSubscriptionTest fixture = new SecureSubscriptionTest();

    @BeforeEach
    void start() throws Exception {
        fixture.dir = dir;
        fixture.startServer();
    }

    @AfterEach
    void stop() throws Exception {
        fixture.closeServer();
    }

    @Test
    @Timeout(30)
    void admitsTheRealSdkInitialNotificationOnlyIfTheConnectionOpens() throws Exception {
        class Processor extends ConsumeOPCUA {
            volatile ConnectionAttempt opening;
            volatile CountDownLatch initial;
            int attempts;

            @Override
            protected AutoCloseable openConnection(
                    ProcessContext context, ConnectionAttempt generation) throws Exception {
                ++attempts;
                initial = new CountDownLatch(1);
                opening = generation;
                AutoCloseable opened = super.openConnection(context, generation);
                try {
                    assertTrue(
                            initial.await(5, TimeUnit.SECONDS),
                            "Initial Publish did not arrive during setup");
                    if (attempts == 1) throw new IllegalStateException("Final setup step failed");
                    return opened;
                } catch (Exception | AssertionError failure) {
                    opened.close();
                    throw failure;
                }
            }

            @Override
            void accept(
                    NotificationBuffer destination,
                    TagDefinition tag,
                    DataValue value,
                    ConnectionAttempt generation) {
                super.accept(destination, tag, value, generation);
                if (generation == opening) initial.countDown();
            }
        }
        NodeId id = Identifiers.Server_ServerStatus_BuildInfo_ManufacturerName;
        UaVariableNode node =
                (UaVariableNode) fixture.server.getAddressSpaceManager().getManagedNode(id).get();
        node.setValue(new DataValue(new Variant("failed")));
        Processor processor = new Processor();
        TestRunner runner = ProcessorTest.configured(processor);
        runner.setProperty(ConsumeOPCUA.ENDPOINT, fixture.endpoint);
        runner.setProperty(ConsumeOPCUA.CERTIFICATE, dir.resolve("client.der").toString());
        runner.setProperty(ConsumeOPCUA.PRIVATE_KEY, dir.resolve("client.pem").toString());
        runner.setProperty(ConsumeOPCUA.PKI, dir.resolve("pki").toString());
        runner.setProperty(ConsumeOPCUA.PASSWORD, "test-password");
        runner.setProperty(ConsumeOPCUA.PUBLISHING, "100");
        runner.removeProperty("DemoSwitch");
        runner.setProperty("manufacturer", id.toParseableString());
        try {
            runner.run(1, false, true);
            runner.assertTransferCount(ConsumeOPCUA.SUCCESS, 0);
            assertTrue(processor.opening.isCancelled());
            assertEquals(0, processor.opening.destination.size());
            node.setValue(new DataValue(new Variant("healthy")));
            processor.primaryChanged(PrimaryNodeState.ELECTED_PRIMARY_NODE);
            runner.run(1, false, false);
            runner.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
            JsonNode batch =
                    new ObjectMapper()
                            .readTree(
                                    runner.getContentAsByteArray(
                                            runner.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS)
                                                    .get(0)));
            assertEquals(1, batch.size());
            assertEquals("healthy", batch.get(0).get("value").asText());
            assertEquals("manufacturer", batch.get(0).get("tag").asText());
            assertEquals(2, processor.attempts);
        } finally {
            runner.shutdown();
        }
    }
}
