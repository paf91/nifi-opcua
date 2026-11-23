/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.*;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.server.items.MonitoredDataItem;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.subscriptions.Subscription;
import org.eclipse.milo.opcua.sdk.server.subscriptions.SubscriptionManager;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.util.Unit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Timeout(75)
class SubscriptionTransferTest {
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void firstValueAfterSuccessfulTransferUsesTheReplacementSessionsTag(boolean metadata)
            throws Exception {
        NodeId nodeId = Identifiers.Server_ServerStatus_BuildInfo_ManufacturerName;
        UaVariableNode node =
                (UaVariableNode)
                        fixture.server.getAddressSpaceManager().getManagedNode(nodeId).get();
        node.setValue(value("initial"));
        CompletableFuture<Boolean> fencedBeforeActive = new CompletableFuture<>();
        AtomicInteger connectionAttempts = new AtomicInteger();
        Field clientField = OpcConnection.class.getDeclaredField("client");
        clientField.setAccessible(true);
        ConsumeOPCUA processor =
                new ConsumeOPCUA() {
                    @Override
                    protected AutoCloseable openConnection(
                            ProcessContext context, ConnectionAttempt generation) throws Exception {
                        connectionAttempts.incrementAndGet();
                        AutoCloseable opened = super.openConnection(context, generation);
                        OpcUaClient client = (OpcUaClient) clientField.get(opened);
                        // The current session is already initialized. Observe the next session
                        // before
                        // its Active listeners can race the transferred subscription's first
                        // Publish.
                        client.addSessionInitializer(
                                (ignored, nextSession) -> {
                                    fencedBeforeActive.complete(generation.isCancelled());
                                    return CompletableFuture.completedFuture(Unit.VALUE);
                                });
                        return opened;
                    }
                };
        TestRunner runner = ProcessorTest.configured(processor);
        runner.setProperty(ConsumeOPCUA.ENDPOINT, fixture.endpoint);
        runner.setProperty(ConsumeOPCUA.CERTIFICATE, dir.resolve("client.der").toString());
        runner.setProperty(ConsumeOPCUA.PRIVATE_KEY, dir.resolve("client.pem").toString());
        runner.setProperty(ConsumeOPCUA.PKI, dir.resolve("pki").toString());
        runner.setProperty(ConsumeOPCUA.PASSWORD, "test-password");
        runner.setProperty(ConsumeOPCUA.PUBLISHING, "100");
        // New items sample immediately. Keep the next periodic sample outside the test timeout.
        runner.setProperty(ConsumeOPCUA.SAMPLING, "120000");
        runner.setProperty(ConsumeOPCUA.RETRY, "100");
        runner.removeProperty("DemoSwitch");
        runner.setProperty("manufacturer", nodeId.toParseableString());
        if (metadata) runner.setProperty(ConsumeOPCUA.OUTPUT_FIELDS, "tag,nodeId,value,dataType");
        try {
            runner.run(1, false, true);
            JsonNode first = firstRecord(runner);
            assertEquals("initial", first.get("value").asText());
            if (metadata)
                assertEquals(
                        Identifiers.String.toParseableString(), first.get("dataType").asText());
            runner.clearTransferState();

            int attemptsBeforeTransfer = connectionAttempts.get();
            assertEquals(1, fixture.server.getSubscriptions().size());
            Subscription subscription =
                    fixture.server.getSubscriptions().values().iterator().next();
            NodeId oldSession = subscription.getSession().getSessionId();
            MonitoredDataItem item =
                    (MonitoredDataItem) subscription.getMonitoredItems().values().iterator().next();
            synchronized (subscription) {
                SubscriptionManager previous = subscription.getSubscriptionManager();
                fixture.server.getSessionManager().killSession(oldSession, false);
                assertTrue(previous.getSubscriptions().isEmpty());
                // Both Milo servers keep outstanding Publish requests when the subscription
                // survives session closure. Complete those old requests before queuing the marker.
                previous.sessionClosed(true);
                assertTrue(previous.getPublishQueue().isEmpty());
                assertSame(
                        subscription, fixture.server.getSubscriptions().get(subscription.getId()));
                item.setValue(value("queued"));
                assertTrue(item.hasNotifications());
                if (metadata) node.setDataType(Identifiers.BaseDataType);
                node.setValue(value(metadata ? "refreshed" : "current"));
            }
            // Keep NiFi idle while Milo transfers the existing subscription to a new session.
            // A processor rebuild cannot satisfy this condition.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!transferred(subscription, oldSession) && System.nanoTime() < deadline)
                Thread.sleep(20);
            assertTrue(
                    transferred(subscription, oldSession),
                    "Original subscription did not transfer");
            assertEquals(
                    metadata,
                    fencedBeforeActive.get(10, TimeUnit.SECONDS),
                    "The old generation must be fenced before Active when its metadata may be"
                            + " stale");
            JsonNode after = firstRecord(runner);
            assertEquals("manufacturer", after.get("tag").asText());
            assertEquals(nodeId.toParseableString(), after.get("nodeId").asText());
            if (metadata)
                assertEquals(
                        Identifiers.BaseDataType.toParseableString(),
                        after.get("dataType").asText(),
                        after.toString());
            assertEquals(
                    metadata ? "refreshed" : "queued",
                    after.get("value").asText(),
                    after.toString());
            if (!metadata) {
                assertEquals(attemptsBeforeTransfer, connectionAttempts.get());
                assertEquals(
                        Collections.singleton(subscription.getId()),
                        fixture.server.getSubscriptions().keySet());
                assertSame(
                        subscription, fixture.server.getSubscriptions().get(subscription.getId()));
            }
        } finally {
            runner.shutdown();
        }
    }

    private boolean transferred(Subscription original, NodeId oldSession) {
        return fixture.server.getSessionManager().getAllSessions().stream()
                .anyMatch(
                        session ->
                                !session.getSessionId().equals(oldSession)
                                        && session.getSubscriptionManager()
                                                        .getSubscription(original.getId())
                                                == original);
    }

    private static DataValue value(String text) {
        return new DataValue(new Variant(text));
    }

    private static JsonNode firstRecord(TestRunner runner) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<MockFlowFile> files = runner.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS);
        while (files.isEmpty() && System.nanoTime() < deadline) {
            runner.run(1, false, false);
            files = runner.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS);
            if (files.isEmpty()) Thread.sleep(20);
        }
        assertFalse(files.isEmpty(), "No FlowFile received");
        return new ObjectMapper().readTree(runner.getContentAsByteArray(files.get(0))).get(0);
    }
}
