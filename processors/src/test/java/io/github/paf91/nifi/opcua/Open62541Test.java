/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.*;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.UserTokenPolicy;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.nio.file.*;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** Runs the independent C server fixture; no external or production endpoint is contacted. */
@EnabledIfEnvironmentVariable(named = "OPCUA_OPEN62541_BINARY", matches = ".+")
class Open62541Test {
    static final String NS = "urn:nifi:interop:tags";
    static final String URI_NODE = "nsu=" + NS + ";s=Value";
    static final String POLICY_PREFIX = "http://opcfoundation.org/UA/SecurityPolicy#";
    @TempDir Path dir;
    Process server;
    int port, sequence;

    @BeforeEach
    void setup() throws Exception {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        for (String role : Arrays.asList("client", "server")) {
            KeyPair pair = generator.generateKeyPair();
            X509Certificate cert =
                    new SelfSignedCertificateBuilder(pair)
                            .setCommonName("Interop test " + role)
                            .setCountryCode("US")
                            .setApplicationUri("urn:nifi:interop:" + role)
                            .addDnsName("localhost")
                            .addIpAddress("127.0.0.1")
                            .build();
            Files.write(dir.resolve(role + ".der"), cert.getEncoded());
            Files.write(dir.resolve(role + "-key.der"), pair.getPrivate().getEncoded());
        }
        Files.createDirectories(dir.resolve("pki/trusted/certs"));
        Files.copy(dir.resolve("server.der"), dir.resolve("pki/trusted/certs/server.der"));
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            port = socket.getLocalPort();
        }
    }

    void start(boolean pad, String tokenPolicy) throws Exception {
        Files.deleteIfExists(dir.resolve("ready.txt"));
        server =
                new ProcessBuilder(
                                System.getenv("OPCUA_OPEN62541_BINARY"),
                                String.valueOf(port),
                                pad ? "1" : "0",
                                POLICY_PREFIX + tokenPolicy)
                        .directory(dir.toFile())
                        .redirectErrorStream(true)
                        .redirectOutput(
                                ProcessBuilder.Redirect.appendTo(
                                        dir.resolve("server.log").toFile()))
                        .start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!Files.exists(dir.resolve("ready.txt"))
                && server.isAlive()
                && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(
                server.isAlive() && Files.exists(dir.resolve("ready.txt")),
                () -> "open62541 startup failed: " + log());
    }

    String log() {
        try {
            return new String(
                    Files.readAllBytes(dir.resolve("server.log")),
                    java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return e.toString();
        }
    }

    @AfterEach
    void stop() throws Exception {
        if (server != null) {
            server.destroy();
            if (!server.waitFor(5, TimeUnit.SECONDS)) {
                server.destroyForcibly();
                server.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    @AfterEach
    void saveLog(TestInfo test) throws Exception {
        if (Files.exists(dir.resolve("server.log"))) {
            Path logs = Paths.get("target", "interop-logs");
            Files.createDirectories(logs);
            Files.copy(
                    dir.resolve("server.log"),
                    logs.resolve(test.getDisplayName().replaceAll("[^a-zA-Z0-9.-]", "_") + ".log"),
                    StandardCopyOption.REPLACE_EXISTING);
        }
    }

    String endpoint() {
        return "opc.tcp://localhost:" + port;
    }

    void assertUsernameTokenPolicy(String policy, String mode, String tokenPolicy)
            throws Exception {
        EndpointDescription endpoint =
                TestDiscovery.getEndpoints(endpoint()).stream()
                        .filter(e -> (POLICY_PREFIX + policy).equals(e.getSecurityPolicyUri()))
                        .filter(e -> MessageSecurityMode.valueOf(mode) == e.getSecurityMode())
                        .findFirst()
                        .orElse(null);
        assertNotNull(endpoint, "No endpoint for " + policy + " / " + mode);
        assertNotNull(endpoint.getUserIdentityTokens(), "Endpoint does not advertise user tokens");
        UserTokenPolicy username =
                Arrays.stream(endpoint.getUserIdentityTokens())
                        .filter(token -> token.getTokenType() == UserTokenType.UserName)
                        .findFirst()
                        .orElse(null);
        assertNotNull(username, "Endpoint does not advertise username authentication");
        assertEquals(POLICY_PREFIX + tokenPolicy, username.getSecurityPolicyUri());
        assertNotNull(username.getPolicyId());
        assertTrue(username.getPolicyId().endsWith("#" + tokenPolicy), username.getPolicyId());
    }

    OpcSettings settings(String policy, String mode, String authentication, String tag) {
        return settings(
                policy, mode, authentication, tag, UnencryptedPasswordPolicy.NONE_CHANNELS_ONLY);
    }

    OpcSettings settings(
            String policy,
            String mode,
            String authentication,
            String tag,
            UnencryptedPasswordPolicy allowance) {
        OpcSecurity security = new OpcSecurity(policy, mode, authentication, allowance);
        return new OpcSettings(
                endpoint(),
                null,
                security.requiresClientIdentity() ? dir.resolve("client.der") : null,
                security.requiresClientIdentity() ? dir.resolve("client-key.der") : null,
                "",
                dir.resolve("pki"),
                "test-user",
                "test-password",
                100,
                2000,
                Collections.singletonList(TagDefinition.parse("value", tag, 50, 10)),
                security);
    }

    static java.util.stream.Stream<Object[]> configurations() {
        return OpcSecurityTest.configurations()
                .filter(c -> "None".equals(c[0]) == "None".equals(c[1]));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cachesOptionalMetadataFromIndependentServer(boolean secure) throws Exception {
        start(false, "None");
        OutputSettings output =
                new OutputSettings(
                        "tag,value,sourceTimestamp,serverTimestamp,dataType,engineeringUnits",
                        "NDJSON",
                        "Epoch Milliseconds",
                        false,
                        0);
        OpcSettings settings =
                settings(
                                secure ? "Basic256Sha256" : "None",
                                secure ? "SignAndEncrypt" : "None",
                                "Anonymous",
                                URI_NODE)
                        .withOutput(output);
        BlockingQueue<JsonNode> records = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ConnectionDiagnostics diagnostics = new ConnectionDiagnostics();
        try (OpcConnection connection =
                new OpcConnection(
                        settings,
                        (tag, value) -> {
                            try {
                                records.offer(
                                        new ObjectMapper()
                                                .readTree(
                                                        NotificationJson.encode(
                                                                tag, value, 4096, output)));
                            } catch (Exception e) {
                                failure.set(e);
                            }
                        },
                        () -> {},
                        () -> {},
                        new ConnectionAttempt(),
                        diagnostics)) {
            JsonNode first = records.poll(5, TimeUnit.SECONDS);
            assertNull(failure.get());
            assertNotNull(first);
            assertEquals(
                    org.eclipse.milo.opcua.stack.core.Identifiers.Double.toParseableString(),
                    first.get("dataType").asText());
            assertEquals(4408652, first.get("engineeringUnits").get("unitId").intValue());
            assertEquals("degC", first.get("engineeringUnits").get("displayName").asText());
            write(73, 0);
            JsonNode next = records.poll(5, TimeUnit.SECONDS);
            assertNotNull(next);
            assertEquals(73.0, next.get("value").doubleValue());
            assertEquals(first.get("engineeringUnits"), next.get("engineeringUnits"));
            assertTrue(next.get("sourceTimestamp").isNumber());
            assertNull(((Map<?, ?>) diagnostics.snapshot().get("tagMetadata")).get("failure"));
        }
    }

    void write(double value, long status) throws Exception {
        int seq = ++sequence;
        Path temporary = dir.resolve("control.tmp");
        Files.write(
                temporary,
                (seq + " " + value + " " + status)
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Files.move(
                temporary,
                dir.resolve("control.txt"),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (Files.exists(dir.resolve("applied.txt"))
                    && String.valueOf(seq)
                            .equals(
                                    new String(
                                            Files.readAllBytes(dir.resolve("applied.txt")),
                                            java.nio.charset.StandardCharsets.UTF_8))) return;
            Thread.sleep(20);
        }
        fail("Server did not apply test value: " + log());
    }

    static DataValue receive(BlockingQueue<DataValue> values) throws Exception {
        DataValue value = values.poll(5, TimeUnit.SECONDS);
        assertNotNull(value, "No data-change notification");
        return value;
    }

    @ParameterizedTest(name = "open62541 {0} / {1} / {2}")
    @MethodSource("configurations")
    void independentSecurityAndAuthenticationMatrix(
            String policy, String mode, String authentication) throws Exception {
        String tokenPolicy =
                "Sign".equals(mode) && OpcSecurity.USERNAME.equals(authentication)
                        ? policy
                        : "None";
        start(false, tokenPolicy);
        if (OpcSecurity.USERNAME.equals(authentication))
            assertUsernameTokenPolicy(policy, mode, tokenPolicy);
        BlockingQueue<DataValue> values = new LinkedBlockingQueue<>();
        ConnectionDiagnostics report = new ConnectionDiagnostics();
        try (OpcConnection connection =
                new OpcConnection(
                        settings(policy, mode, authentication, URI_NODE),
                        (tag, value) -> values.offer(value),
                        () -> {},
                        () -> {},
                        new ConnectionAttempt(),
                        report)) {
            assertTrue(receive(values).getStatusCode().isGood());
            write(42, 0);
            assertEquals(42.0, ((Number) receive(values).getValue().getValue()).doubleValue());
            assertEquals("Subscribed", report.snapshot().get("stage"));
            assertEquals(true, report.snapshot().get("sessionActive"));
            assertEquals(1, report.snapshot().get("monitoredItemCount"));
            assertNotNull(report.snapshot().get("serverCertificate"));
        }
        values.clear();
        write(73, 0);
        assertNull(values.poll(350, TimeUnit.MILLISECONDS), "Callback after connection close");
        assertEquals(false, report.snapshot().get("sessionActive"));
    }

    @Test
    void encryptsUsernameTokensEvenWithoutChannelSecurity() throws Exception {
        start(false, "Basic256Sha256");
        assertUsernameTokenPolicy("None", "None", "Basic256Sha256");
        BlockingQueue<DataValue> values = new LinkedBlockingQueue<>();
        try (OpcConnection connection =
                new OpcConnection(
                        settings("None", "None", OpcSecurity.USERNAME, URI_NODE),
                        (tag, value) -> values.offer(value),
                        () -> {},
                        () -> {})) {
            assertTrue(receive(values).getStatusCode().isGood());
        }
    }

    @Test
    void signedChannelRejectsPlaintextPasswordByDefaultAndAllowsTheExplicitOverride()
            throws Exception {
        start(false, "None");
        assertUsernameTokenPolicy("Basic256Sha256", "Sign", "None");
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            try (OpcConnection connection =
                                    new OpcConnection(
                                            settings(
                                                    "Basic256Sha256",
                                                    "Sign",
                                                    OpcSecurity.USERNAME,
                                                    URI_NODE),
                                            (tag, value) -> {},
                                            () -> {},
                                            () -> {})) {}
                        });
        assertTrue(failure.getMessage().contains("Allow Unencrypted Password"));
        BlockingQueue<DataValue> values = new LinkedBlockingQueue<>();
        try (OpcConnection connection =
                new OpcConnection(
                        settings(
                                "Basic256Sha256",
                                "Sign",
                                OpcSecurity.USERNAME,
                                URI_NODE,
                                UnencryptedPasswordPolicy.ALWAYS),
                        (tag, value) -> values.offer(value),
                        () -> {},
                        () -> {})) {
            assertTrue(receive(values).getStatusCode().isGood());
            write(42, 0);
            assertEquals(42.0, ((Number) receive(values).getValue().getValue()).doubleValue());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"Absolute", "Percent"})
    void serverDeadbandsSuppressSmallChangesButDeliverQualityChanges(String type) throws Exception {
        start(false, "None");
        // Keep the status field present so this test isolates the value deadband.
        write(0, 0);
        String tag =
                "{\"nodeId\":\""
                        + URI_NODE
                        + "\",\"deadbandType\":\""
                        + type
                        + "\",\"deadbandValue\":5}";
        BlockingQueue<DataValue> values = new LinkedBlockingQueue<>();
        try (OpcConnection connection =
                new OpcConnection(
                        settings("None", "None", OpcSecurity.ANONYMOUS, tag),
                        (t, value) -> values.offer(value),
                        () -> {},
                        () -> {})) {
            assertEquals(0.0, ((Number) receive(values).getValue().getValue()).doubleValue());
            write(3, 0);
            assertNull(values.poll(400, TimeUnit.MILLISECONDS));
            write(5, 0);
            assertNull(values.poll(400, TimeUnit.MILLISECONDS)); // threshold is strictly exceeded
            write(6, 0);
            assertEquals(6.0, ((Number) receive(values).getValue().getValue()).doubleValue());
            write(6.1, 0x40000000L);
            assertEquals(0x40000000L, receive(values).getStatusCode().getValue());
            write(6.2, 0);
            assertTrue(receive(values).getStatusCode().isGood());
        }
    }

    @Test
    void rejectsPercentDeadbandWithoutEngineeringRange() throws Exception {
        start(false, "None");
        String tag =
                "{\"nodeId\":\"nsu="
                        + NS
                        + ";s=NoRange\",\"deadbandType\":\"Percent\",\"deadbandValue\":5}";
        Exception error =
                assertThrows(
                        Exception.class,
                        () ->
                                new OpcConnection(
                                        settings("None", "None", OpcSecurity.ANONYMOUS, tag),
                                        (t, value) -> {},
                                        () -> {},
                                        () -> {}));
        assertTrue(
                error.toString().contains("Bad") || error.toString().contains("StatusCode"),
                error.toString());
    }

    TestRunner processor(ConsumeOPCUA p) {
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.ENDPOINT, endpoint());
        r.setProperty(ConsumeOPCUA.SECURITY_POLICY, "None");
        r.setProperty(ConsumeOPCUA.SECURITY_MODE, "None");
        r.setProperty(ConsumeOPCUA.AUTHENTICATION, OpcSecurity.ANONYMOUS);
        r.removeProperty("DemoSwitch");
        r.setProperty("value", URI_NODE);
        r.setProperty(ConsumeOPCUA.PUBLISHING, "100");
        r.setProperty(ConsumeOPCUA.SAMPLING, "50");
        r.setProperty(ConsumeOPCUA.RETRY, "100");
        r.setProperty(ConsumeOPCUA.TIMEOUT, "1500");
        return r;
    }

    @Test
    void verificationChecksLiveTagsAndRejectsMissingNamespaceAndDuplicates() throws Exception {
        start(false, "None");
        ConsumeOPCUA p = new ConsumeOPCUA();
        TestRunner r = processor(p);
        assertEquals(
                ConfigVerificationResult.Outcome.SUCCESSFUL,
                p.verify(r.getProcessContext(), null, Collections.emptyMap()).get(0).getOutcome());
        r.setProperty("duplicate", "ns=2;s=Value");
        ConfigVerificationResult duplicate =
                p.verify(r.getProcessContext(), null, Collections.emptyMap()).get(0);
        assertEquals(ConfigVerificationResult.Outcome.FAILED, duplicate.getOutcome());
        assertTrue(duplicate.getExplanation().contains("same NodeId"));
        r.removeProperty("duplicate");
        r.setProperty("value", "nsu=urn:missing;s=Value");
        assertEquals(
                ConfigVerificationResult.Outcome.FAILED,
                p.verify(r.getProcessContext(), null, Collections.emptyMap()).get(0).getOutcome());
    }

    @Test
    void resolvesMovedNamespaceAfterServerRestartBeforeAcceptingNewValues() throws Exception {
        start(false, "None");
        ConsumeOPCUA p = new ConsumeOPCUA();
        TestRunner r = processor(p);
        try {
            r.run(1, false, true);
            awaitRecord(r, 0, 5);
            stop();
            Thread.sleep(200);
            start(true, "None");
            write(42, 0);
            boolean fresh = false;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            while (!fresh && System.nanoTime() < deadline) {
                r.clearTransferState();
                r.run(1, false, false);
                for (MockFlowFile flow : r.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS)) {
                    for (JsonNode record :
                            new ObjectMapper().readTree(r.getContentAsByteArray(flow))) {
                        assertNotEquals(
                                -9999.0,
                                record.get("value").asDouble(),
                                "Subscribed using a stale namespace index");
                        if (record.get("value").asDouble() == 42.0) {
                            assertEquals("ns=3;s=Value", record.get("nodeId").asText());
                            assertEquals(URI_NODE, record.get("configuredNodeId").asText());
                            fresh = true;
                        }
                    }
                }
                if (!fresh) Thread.sleep(100);
            }
            assertTrue(fresh, () -> "No values after namespace moved: " + log());
        } finally {
            r.shutdown();
        }
    }

    @Test
    void metadataRefreshesWithNamespaceIndexesAfterRestartAndFencesOldGeneration()
            throws Exception {
        start(false, "None");
        assertEquals(
                "2",
                new String(
                        Files.readAllBytes(dir.resolve("ready.txt")),
                        java.nio.charset.StandardCharsets.UTF_8));
        CountDownLatch oldCallback = new CountDownLatch(1), releaseOld = new CountDownLatch(1);
        CountDownLatch oldReturned = new CountDownLatch(1);
        AtomicReference<ConnectionAttempt> firstGeneration = new AtomicReference<>();
        AtomicReference<ConnectionAttempt> latestGeneration = new AtomicReference<>();
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        ConsumeOPCUA processor =
                new ConsumeOPCUA() {
                    @Override
                    protected AutoCloseable openConnection(
                            ProcessContext context, ConnectionAttempt generation) throws Exception {
                        firstGeneration.compareAndSet(null, generation);
                        latestGeneration.set(generation);
                        return super.openConnection(context, generation);
                    }

                    @Override
                    void accept(
                            NotificationBuffer destination,
                            TagDefinition tag,
                            DataValue value,
                            ConnectionAttempt generation) {
                        Object raw = value.getValue() == null ? null : value.getValue().getValue();
                        boolean held =
                                generation == firstGeneration.get()
                                        && raw instanceof Number
                                        && ((Number) raw).doubleValue() == 91;
                        if (held) {
                            oldCallback.countDown();
                            try {
                                if (!releaseOld.await(40, TimeUnit.SECONDS))
                                    callbackFailure.set(
                                            new AssertionError("Old callback was not released"));
                            } catch (InterruptedException failure) {
                                Thread.currentThread().interrupt();
                                callbackFailure.set(failure);
                            }
                        }
                        super.accept(destination, tag, value, generation);
                        if (held) oldReturned.countDown();
                    }
                };
        TestRunner runner = processor(processor);
        runner.setProperty("value", "ns=2;s=Value");
        runner.setProperty(
                ConsumeOPCUA.OUTPUT_FIELDS,
                "tag,nodeId,configuredNodeId,value,dataType,engineeringUnits");
        try {
            runner.run(1, false, true);
            JsonNode original = awaitRecord(runner, 0, 5);
            assertEquals("degC", original.get("engineeringUnits").get("displayName").asText());
            assertEquals("ns=2;s=Value", original.get("nodeId").asText());
            assertFalse(original.has("configuredNodeId"));
            runner.clearTransferState();
            write(91, 0);
            assertTrue(oldCallback.await(5, TimeUnit.SECONDS));
            stop();
            Files.createFile(dir.resolve("metadata-revision.flag"));
            start(false, "None");
            assertEquals(
                    "2",
                    new String(
                            Files.readAllBytes(dir.resolve("ready.txt")),
                            java.nio.charset.StandardCharsets.UTF_8));
            write(42, 0);
            JsonNode refreshed = awaitRecord(runner, 42, 25);
            assertNotSame(firstGeneration.get(), latestGeneration.get());
            assertTrue(firstGeneration.get().isCancelled());
            assertEquals("ns=2;s=Value", refreshed.get("nodeId").asText());
            assertFalse(refreshed.has("configuredNodeId"));
            Map<?, ?> connection =
                    (Map<?, ?>)
                            processor
                                    .diagnosticSnapshot(runner.getProcessContext())
                                    .get("connection");
            List<?> items = (List<?>) connection.get("monitoredItems");
            assertEquals(1, items.size());
            assertEquals("ns=2;s=Value", ((Map<?, ?>) items.get(0)).get("configuredNodeId"));
            assertEquals("ns=2;s=Value", ((Map<?, ?>) items.get(0)).get("resolvedNodeId"));
            assertEquals(
                    org.eclipse.milo.opcua.stack.core.Identifiers.Double.toParseableString(),
                    refreshed.get("dataType").asText());
            assertEquals(4408652, refreshed.get("engineeringUnits").get("unitId").intValue());
            assertEquals(
                    "degC updated", refreshed.get("engineeringUnits").get("displayName").asText());
            assertEquals(
                    "updated Celsius metadata",
                    refreshed.get("engineeringUnits").get("description").asText());
            releaseOld.countDown();
            assertTrue(oldReturned.await(5, TimeUnit.SECONDS));
            assertNull(callbackFailure.get());
            write(43, 0);
            JsonNode freshValue = awaitRecord(runner, 43, 5);
            assertEquals(refreshed.get("engineeringUnits"), freshValue.get("engineeringUnits"));
            for (JsonNode record : deliveredRecords(runner))
                assertNotEquals(
                        91.0,
                        record.get("value").doubleValue(),
                        "Obsolete generation entered the buffer");
            assertTrue(
                    ((Number)
                                            processor
                                                    .diagnosticSnapshot(runner.getProcessContext())
                                                    .get("successfulConnections"))
                                    .longValue()
                            >= 2);
        } finally {
            releaseOld.countDown();
            runner.shutdown();
        }
    }

    private static JsonNode awaitRecord(TestRunner runner, double expected, int timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        do {
            for (JsonNode record : deliveredRecords(runner))
                if (record.hasNonNull("value") && record.get("value").asDouble() == expected)
                    return record;
            runner.run(1, false, false);
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        fail("No delivered value " + expected + " within " + timeoutSeconds + " seconds");
        return null;
    }

    private static List<JsonNode> deliveredRecords(TestRunner runner) throws Exception {
        List<JsonNode> records = new ArrayList<>();
        ObjectMapper json = new ObjectMapper();
        for (MockFlowFile flow : runner.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS))
            for (JsonNode record : json.readTree(runner.getContentAsByteArray(flow)))
                records.add(record);
        return records;
    }

    @Test
    void closingDoesNotWaitForAConsumerAndFencesFurtherDiagnostics() throws Exception {
        start(false, "None");
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        ConnectionDiagnostics report = new ConnectionDiagnostics();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        OpcConnection connection =
                new OpcConnection(
                        settings("None", "None", OpcSecurity.ANONYMOUS, URI_NODE),
                        (tag, value) -> {
                            if (((Number) value.getValue().getValue()).doubleValue() == 99) {
                                entered.countDown();
                                try {
                                    assertTrue(release.await(10, TimeUnit.SECONDS));
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        },
                        () -> {},
                        () -> {},
                        new ConnectionAttempt(),
                        report);
        try {
            write(99, 0);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            CountDownLatch closing = new CountDownLatch(1);
            Future<?> closed =
                    worker.submit(
                            () -> {
                                closing.countDown();
                                connection.close();
                            });
            assertTrue(closing.await(1, TimeUnit.SECONDS));
            // Encoding may still be running. Closing must finish without waiting for it;
            // the processor independently fences final queue admission after encoding.
            closed.get(3, TimeUnit.SECONDS);
            assertEquals(false, report.snapshot().get("sessionActive"));
            long received = (Long) report.snapshot().get("notificationsReceived");
            release.countDown();
            write(100, 0);
            Thread.sleep(300);
            assertEquals(received, report.snapshot().get("notificationsReceived"));
        } finally {
            release.countDown();
            connection.close();
            worker.shutdownNow();
        }
    }

    @Test
    void connectsWithAnUnadvertisedTwoItemLimit() throws Exception {
        Files.createFile(dir.resolve("unadvertised-limit.flag"));
        start(false, "None");
        ConsumeOPCUA processor = new ConsumeOPCUA();
        TestRunner runner = processor(processor);
        runner.setProperty("other", "nsu=" + NS + ";s=NoRange");
        runner.setProperty("range", "nsu=" + NS + ";s=Value.EURange");
        ConfigVerificationResult result =
                processor.verify(runner.getProcessContext(), null, Collections.emptyMap()).get(0);
        assertEquals(
                ConfigVerificationResult.Outcome.SUCCESSFUL,
                result.getOutcome(),
                result.getExplanation());
    }
}
