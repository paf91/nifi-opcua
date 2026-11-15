/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.*;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.notification.*;
import org.apache.nifi.components.*;
import org.apache.nifi.processor.*;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Modifier;
import java.util.*;

class ProcessorContractTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TagDefinition TAG =
            TagDefinition.parse("DemoSwitch", ProcessorTest.NODE, 1000, 100);

    private static final class Tracking extends ConsumeOPCUA {
        int opened;
        int closed;
        int verificationOpened;
        int verificationClosed;
        boolean failVerificationClose;
        ConnectionAttempt generation;
        ConnectionAttempt verificationGeneration;

        @Override
        protected AutoCloseable openConnection(ProcessContext context, ConnectionAttempt attempt) {
            generation = attempt;
            opened++;
            return () -> closed++;
        }

        @Override
        protected AutoCloseable verifyConnection(
                OpcSettings settings, ConnectionAttempt attempt, ConnectionDiagnostics report) {
            verificationGeneration = attempt;
            verificationOpened++;
            report.stage("Subscribed");
            report.namespaces(
                    new String[] {
                        "urn:test:" + settings.password(),
                        "urn:key:" + settings.privateKeyPassword(),
                        "urn:client-store:" + settings.clientKeyStore().password(),
                        "urn:trust-store:" + settings.serverTrustStore().password()
                    });
            return () -> {
                verificationClosed++;
                if (failVerificationClose)
                    throw new IllegalStateException(
                            "verification close failed: "
                                    + settings.password()
                                    + "; "
                                    + settings.privateKeyPassword()
                                    + "; "
                                    + settings.clientKeyStore().password()
                                    + "; "
                                    + settings.serverTrustStore().password());
            };
        }
    }

    private static final class Fixture implements AutoCloseable {
        final Tracking processor = new Tracking();
        final TestRunner runner = ProcessorTest.configured(processor);

        void emit(int value) {
            emit(processor.generation, value);
        }

        void emit(ConnectionAttempt source, int value) {
            source.ifActive(() -> processor.acceptForTest(TAG, new DataValue(new Variant(value))));
        }

        @Override
        public void close() {
            runner.unSchedule();
            runner.stop();
            runner.shutdown();
        }
    }

    @Test
    void publicProcessorExposesTheRequiredNiFiMetadata() throws Exception {
        Class<ConsumeOPCUA> implementation = ConsumeOPCUA.class;
        assertTrue(Modifier.isPublic(implementation.getModifiers()));
        ConsumeOPCUA processor = implementation.getConstructor().newInstance();
        TestRunner runner = ProcessorTest.configured(processor);
        runner.assertValid();
        assertSame(processor, runner.getProcessor());
        assertEquals(
                InputRequirement.Requirement.INPUT_FORBIDDEN,
                implementation.getAnnotation(InputRequirement.class).value());
        assertNotNull(implementation.getAnnotation(TriggerSerially.class));
        assertNotNull(implementation.getAnnotation(PrimaryNodeOnly.class));
        DefaultSchedule schedule = implementation.getAnnotation(DefaultSchedule.class);
        assertEquals("100 ms", schedule.period());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, schedule.strategy());
        assertEquals(1, schedule.concurrentTasks());
        assertArrayEquals(
                new String[] {"opcua", "opc", "subscription", "industrial", "iot"},
                implementation.getAnnotation(Tags.class).value());
        assertEquals("Tag alias", implementation.getAnnotation(DynamicProperty.class).name());
        assertNull(implementation.getAnnotation(DeprecationNotice.class));
        Set<RequiredPermission> permissions = EnumSet.noneOf(RequiredPermission.class);
        for (Restriction restriction :
                implementation.getAnnotation(Restricted.class).restrictions()) {
            assertFalse(restriction.explanation().isEmpty());
            permissions.add(restriction.requiredPermission());
        }
        assertEquals(
                EnumSet.of(RequiredPermission.READ_FILESYSTEM, RequiredPermission.WRITE_FILESYSTEM),
                permissions);
        assertNotNull(implementation.getDeclaredAnnotation(CapabilityDescription.class));
        runner.shutdown();
    }

    @Test
    void lifecycleHooksScheduleDeliverStopAndRestartThroughTestRunner() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.runner.run(0, false, true);
            assertEquals(0, fixture.processor.opened);
            fixture.runner.run(1, false, false);
            ConnectionAttempt first = fixture.processor.generation;
            assertEquals(1, fixture.processor.opened);
            assertFalse(first.isCancelled());
            fixture.emit(41);
            fixture.runner.run(1, false, false);
            assertValues(fixture.runner, 0, 41);
            fixture.runner.unSchedule();
            assertTrue(first.isCancelled());
            assertEquals(1, fixture.processor.closed);
            fixture.runner.stop();
            assertEquals(1, fixture.processor.closed);
            fixture.emit(first, 999);
            fixture.runner.run(1, false, true);
            assertEquals(2, fixture.processor.opened);
            assertNotSame(first, fixture.processor.generation);
            fixture.emit(42);
            fixture.runner.run(1, true, false);
            assertTrue(fixture.processor.generation.isCancelled());
            assertEquals(2, fixture.processor.closed);
            fixture.runner.assertTransferCount(ConsumeOPCUA.SUCCESS, 2);
            assertValues(fixture.runner, 1, 42);
            assertEquals(2, fixture.runner.getProvenanceEvents().size());
        }
    }

    @Test
    void primaryAndRemovalAnnotationsCancelAndFenceConnections() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.runner.setIsConfiguredForClustering(true);
            fixture.runner.setClustered(true);
            fixture.runner.setConnected(true);
            fixture.runner.setPrimaryNode(true);
            fixture.runner.run(1, false, true);
            ConnectionAttempt first = fixture.processor.generation;
            fixture.runner.setPrimaryNode(false);
            ReflectionUtils.invokeMethodsWithAnnotation(
                    OnPrimaryNodeStateChange.class,
                    fixture.processor,
                    PrimaryNodeState.PRIMARY_NODE_REVOKED);
            assertTrue(first.isCancelled());
            assertEquals(1, fixture.processor.closed);
            fixture.emit(first, 999);
            fixture.runner.run(1, false, false);
            assertEquals(1, fixture.processor.opened);
            fixture.runner.assertTransferCount(ConsumeOPCUA.SUCCESS, 0);
            fixture.runner.setPrimaryNode(true);
            ReflectionUtils.invokeMethodsWithAnnotation(
                    OnPrimaryNodeStateChange.class,
                    fixture.processor,
                    PrimaryNodeState.ELECTED_PRIMARY_NODE);
            fixture.runner.run(1, false, false);
            ConnectionAttempt second = fixture.processor.generation;
            assertNotSame(first, second);
            assertEquals(2, fixture.processor.opened);
            fixture.emit(100);
            ReflectionUtils.invokeMethodsWithAnnotation(OnRemoved.class, fixture.processor);
            assertTrue(second.isCancelled());
            assertEquals(2, fixture.processor.closed);
            fixture.emit(second, 999);
            fixture.runner.run(1, false, true);
            assertEquals(3, fixture.processor.opened);
            fixture.runner.assertTransferCount(ConsumeOPCUA.SUCCESS, 0);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void verificationCleansUpRedactsPasswordsAndPreservesDelivery(boolean failOnClose)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            String password = "test-user\nsecret";
            String keyPassword = "test-key\rsecret";
            String storePassword = "test-store\nsecret";
            String trustPassword = "test-trust\rsecret";
            fixture.runner.setProperty(ConsumeOPCUA.PASSWORD, password);
            fixture.runner.setProperty(ConsumeOPCUA.KEY_PASSWORD, keyPassword);
            fixture.runner.setProperty(ConsumeOPCUA.CLIENT_IDENTITY_FORMAT, "JKS");
            fixture.runner.removeProperty(ConsumeOPCUA.CERTIFICATE);
            fixture.runner.removeProperty(ConsumeOPCUA.PRIVATE_KEY);
            fixture.runner.setProperty(ConsumeOPCUA.KEYSTORE, "/tmp/test-client.jks");
            fixture.runner.setProperty(ConsumeOPCUA.KEYSTORE_PASSWORD, storePassword);
            fixture.runner.setProperty(ConsumeOPCUA.SERVER_TRUST_SOURCE, "PKCS12");
            fixture.runner.setProperty(ConsumeOPCUA.TRUSTSTORE, "/tmp/test-trust.p12");
            fixture.runner.setProperty(ConsumeOPCUA.TRUSTSTORE_PASSWORD, trustPassword);
            fixture.processor.failVerificationClose = failOnClose;
            fixture.runner.run(1, false, true);
            ConnectionAttempt live = fixture.processor.generation;
            fixture.emit(73);
            ConfigVerificationResult result =
                    fixture.processor
                            .verify(
                                    fixture.runner.getProcessContext(),
                                    fixture.runner.getLogger(),
                                    Collections.emptyMap())
                            .get(0);
            assertEquals(
                    failOnClose
                            ? ConfigVerificationResult.Outcome.FAILED
                            : ConfigVerificationResult.Outcome.SUCCESSFUL,
                    result.getOutcome());
            assertEquals(1, fixture.processor.verificationOpened);
            assertEquals(1, fixture.processor.verificationClosed);
            assertTrue(fixture.processor.verificationGeneration.isCancelled());
            assertNotSame(live, fixture.processor.verificationGeneration);
            assertFalse(live.isCancelled());
            assertEquals(0, fixture.processor.closed);
            String explanation = result.getExplanation();
            assertTrue(explanation.contains("[redacted]"));
            assertFalse(explanation.contains("test-user"));
            assertFalse(explanation.contains("test-key"));
            assertFalse(explanation.contains("test-store"));
            assertFalse(explanation.contains("test-trust"));
            assertFalse(explanation.contains("secret"));
            assertFalse(explanation.contains("\n"));
            assertFalse(explanation.contains("\r"));
            assertEquals(
                    1L,
                    fixture.processor
                            .diagnosticSnapshot(fixture.runner.getProcessContext())
                            .get("connectionAttempts"));
            fixture.runner.run(1, false, false);
            fixture.runner.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
            fixture.runner.assertTransferCount(ConsumeOPCUA.DIAGNOSTICS, 0);
            assertValues(fixture.runner, 0, 73);
            assertEquals(1, fixture.processor.opened);
            assertFalse(live.isCancelled());
        }
    }

    @Test
    void processorInstancesKeepBuffersAndConnectionsIndependent() throws Exception {
        try (Fixture first = new Fixture();
                Fixture second = new Fixture()) {
            first.runner.setProperty(ConsumeOPCUA.ENDPOINT, "opc.tcp://first:4840");
            second.runner.setProperty(ConsumeOPCUA.ENDPOINT, "opc.tcp://second:4840");
            first.runner.run(1, false, true);
            second.runner.run(1, false, true);
            ConnectionAttempt secondGeneration = second.processor.generation;
            assertNotSame(first.processor.generation, secondGeneration);
            first.emit(101);
            second.emit(202);
            first.runner.run(1, false, false);
            assertValues(first.runner, 0, 101);
            first.runner.unSchedule();
            first.runner.stop();
            assertEquals(1, first.processor.closed);
            assertEquals(0, second.processor.closed);
            assertFalse(secondGeneration.isCancelled());
            second.emit(203);
            second.runner.run(1, false, false);
            second.runner.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
            assertValues(second.runner, 0, 202, 203);
            second.runner
                    .getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS)
                    .get(0)
                    .assertAttributeEquals("opcua.endpoint", "opc.tcp://second:4840");
            first.runner.run(1, false, true);
            first.emit(104);
            first.runner.run(1, true, false);
            assertValues(first.runner, 1, 104);
            assertEquals(2, first.processor.opened);
            assertEquals(2, first.processor.closed);
            assertEquals(1, second.processor.opened);
            assertEquals(0, second.processor.closed);
            assertSame(secondGeneration, second.processor.generation);
            assertFalse(secondGeneration.isCancelled());
        }
    }

    private static void assertValues(TestRunner runner, int flowFile, int... values)
            throws Exception {
        JsonNode records =
                JSON.readTree(
                        runner.getContentAsByteArray(
                                runner.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS)
                                        .get(flowFile)));
        assertEquals(values.length, records.size());
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], records.get(i).get("value").asInt());
            assertEquals(ProcessorTest.NODE, records.get(i).get("nodeId").asText());
        }
    }
}
