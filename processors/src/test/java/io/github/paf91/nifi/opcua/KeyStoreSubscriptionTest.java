/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

class KeyStoreSubscriptionTest {
    @TempDir Path dir;
    private SecureSubscriptionTest fixture;

    enum Configuration {
        PEM_PROTECTED_JKS("PEM/DER", "", "", "JKS", "trust-secret", false, false, false),
        EMPTY_JKS_PKI("JKS", "", "", null, "", false, false, false),
        DISTINCT_JKS_EMPTY_JKS("JKS", "store-secret", "key-secret", "JKS", "", false, false, false),
        PEM_PROTECTED_PKCS12("PEM/DER", "", "", "PKCS12", "trust-secret", false, false, false),
        NO_MAC_PKCS12_PKI("PKCS12", "", "", null, "", true, false, false),
        DISTINCT_PKCS12_NO_MAC_PKCS12(
                "PKCS12", "store-secret", "key-secret", "PKCS12", "", false, true, false),
        NONE_ENCRYPTED_USERNAME_EMPTY_JKS(null, "", "", "JKS", "", false, false, true),
        NONE_ENCRYPTED_USERNAME_NO_MAC_PKCS12(null, "", "", "PKCS12", "", false, true, true);

        final String identityType;
        final String storePassword;
        final String keyPassword;
        final String trustType;
        final String trustPassword;
        final boolean noMacIdentity;
        final boolean noMacTrust;
        final boolean tokenOnly;

        Configuration(
                String identityType,
                String storePassword,
                String keyPassword,
                String trustType,
                String trustPassword,
                boolean noMacIdentity,
                boolean noMacTrust,
                boolean tokenOnly) {
            this.identityType = identityType;
            this.storePassword = storePassword;
            this.keyPassword = keyPassword;
            this.trustType = trustType;
            this.trustPassword = trustPassword;
            this.noMacIdentity = noMacIdentity;
            this.noMacTrust = noMacTrust;
            this.tokenOnly = tokenOnly;
        }
    }

    @ParameterizedTest
    @EnumSource(Configuration.class)
    void liveSubscriptionsUseTheSelectedIdentityAndTrustStores(Configuration configuration)
            throws Exception {
        start(configuration.tokenOnly);
        OpcSettings settings = settings(configuration, "test-password");
        ClientIdentity identity = ClientIdentity.load(settings);
        if (configuration.tokenOnly) assertNull(identity);
        else {
            assertEquals(fixture.clientCert, identity.chain()[0]);
            assertArrayEquals(
                    fixture.clientKey.getPrivate().getEncoded(),
                    identity.keyPair().getPrivate().getEncoded());
            assertEquals("urn:test:client", identity.applicationUri());
        }
        byte[] identityBytes =
                settings.clientKeyStore() == null
                        ? null
                        : Files.readAllBytes(settings.clientKeyStore().path());
        byte[] trustBytes =
                settings.serverTrustStore() == null
                        ? null
                        : Files.readAllBytes(settings.serverTrustStore().path());
        BlockingQueue<DataValue> values = new LinkedBlockingQueue<>();
        ConnectionDiagnostics report = new ConnectionDiagnostics();
        try (OpcConnection connection = connect(settings, report, values)) {
            DataValue value = values.poll(10, TimeUnit.SECONDS);
            assertNotNull(value, "The subscription must deliver a real monitored value");
            assertTrue(value.getStatusCode().isGood(), value.toString());
            assertNotNull(value.getValue().getValue());
            assertEquals("Subscribed", report.snapshot().get("stage"));
        }
        if (identityBytes != null)
            assertArrayEquals(identityBytes, Files.readAllBytes(settings.clientKeyStore().path()));
        if (trustBytes != null)
            assertArrayEquals(trustBytes, Files.readAllBytes(settings.serverTrustStore().path()));
    }

    static Stream<Object[]> rejectedConfigurations() {
        return Stream.of("JKS", "PKCS12")
                .flatMap(
                        type ->
                                Stream.of(
                                                "unrelated-trust",
                                                "store-password",
                                                "key-password",
                                                "trust-password",
                                                "login")
                                        .map(problem -> new Object[] {type, problem}));
    }

    @ParameterizedTest(name = "{0} / reject {1}")
    @MethodSource("rejectedConfigurations")
    void badTrustOrPasswordsNeverDeliverMonitoredValues(String type, String problem)
            throws Exception {
        start(false);
        Path identityFile =
                KeyStoreFixtures.identity(
                        dir.resolve("client.store"),
                        type,
                        "store-secret",
                        "key-secret",
                        "client",
                        fixture.clientKey,
                        fixture.clientCert);
        Path trustFile =
                KeyStoreFixtures.trust(
                        dir.resolve("trust.store"),
                        type,
                        "trust-secret",
                        "unrelated-trust".equals(problem)
                                ? KeyStoreFixtures.OTHER.certificate
                                : fixture.serverCert);
        byte[] identityBytes = Files.readAllBytes(identityFile);
        byte[] trustBytes = Files.readAllBytes(trustFile);
        OpcSettings settings =
                base(
                                "login".equals(problem) ? "wrong-login" : "test-password",
                                "key-password".equals(problem) ? "wrong-key" : "key-secret",
                                false)
                        .withKeyStores(
                                new KeyStoreSettings(
                                        type,
                                        identityFile,
                                        "store-password".equals(problem)
                                                ? "wrong-store"
                                                : "store-secret"),
                                "client",
                                new KeyStoreSettings(
                                        type,
                                        trustFile,
                                        "trust-password".equals(problem)
                                                ? "wrong-trust"
                                                : "trust-secret"));
        BlockingQueue<DataValue> values = new LinkedBlockingQueue<>();
        Exception failure =
                assertThrows(
                        Exception.class,
                        () -> {
                            try (OpcConnection connection =
                                    connect(settings, new ConnectionDiagnostics(), values)) {
                                // Closing an unexpectedly accepted connection prevents fixture
                                // leakage.
                            }
                        });
        if ("login".equals(problem)) {
            StatusCode code = UaException.extractStatusCode(failure).orElse(null);
            assertNotNull(code, failure.toString());
            assertTrue(
                    code.getValue() == StatusCodes.Bad_UserAccessDenied
                            || code.getValue() == StatusCodes.Bad_IdentityTokenInvalid
                            || code.getValue() == StatusCodes.Bad_IdentityTokenRejected,
                    failure.toString());
        }
        assertNull(
                values.poll(200, TimeUnit.MILLISECONDS),
                "Rejected sessions must not publish values");
        assertArrayEquals(identityBytes, Files.readAllBytes(identityFile));
        assertArrayEquals(trustBytes, Files.readAllBytes(trustFile));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void disabledServerValidationDoesNotOpenConfiguredTrustStoreOrPkiDirectory(String type)
            throws Exception {
        start(false);
        Path pkiFile = dir.resolve("pki-is-a-file");
        byte[] marker = new byte[] {1, 2, 3};
        Files.write(pkiFile, marker);
        Path storeFile = dir.resolve("unused-trust.store");
        if ("PKCS12".equals(type)) Files.write(storeFile, marker);
        OpcSettings settings =
                new OpcSettings(
                                fixture.endpoint,
                                null,
                                dir.resolve("client.der"),
                                dir.resolve("client.pem"),
                                "",
                                pkiFile,
                                "user",
                                "test-password",
                                100,
                                5000,
                                Collections.singletonList(
                                        TagDefinition.parse("time", "ns=0;i=2258", 100, 10)),
                                OpcSecurity.DEFAULT,
                                ServerCertificateValidation.NONE)
                        .withKeyStores(
                                null,
                                null,
                                new KeyStoreSettings(type, storeFile, "wrong-password"));
        assertEquals(fixture.clientCert, ClientIdentity.load(settings).chain()[0]);
        BlockingQueue<DataValue> values = new LinkedBlockingQueue<>();
        ConnectionDiagnostics report = new ConnectionDiagnostics();
        try (OpcConnection connection = connect(settings, report, values)) {
            DataValue value = values.poll(10, TimeUnit.SECONDS);
            assertNotNull(value);
            assertTrue(value.getStatusCode().isGood(), value.toString());
            assertEquals("None", report.snapshot().get("serverCertificateValidation"));
        }
        assertArrayEquals(marker, Files.readAllBytes(pkiFile));
        if ("PKCS12".equals(type)) assertArrayEquals(marker, Files.readAllBytes(storeFile));
        else assertFalse(Files.exists(storeFile), "Unused truststore must not be created");
    }

    private void start(boolean tokenOnly) throws Exception {
        fixture = new SecureSubscriptionTest();
        fixture.dir = dir;
        fixture.startServer(tokenOnly ? SecurityPolicy.Basic256Sha256.getUri() : null);
    }

    private OpcSettings settings(Configuration configuration, String loginPassword)
            throws Exception {
        KeyStoreSettings client = null;
        if (configuration.identityType != null && !"PEM/DER".equals(configuration.identityType)) {
            Path file = dir.resolve("client.store");
            if (configuration.noMacIdentity) {
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        file,
                        fixture.clientKey,
                        fixture.clientCert,
                        "client",
                        configuration.keyPassword,
                        null);
                KeyStoreTest.assertUnencryptedSafeContents(file, false, true);
            } else
                KeyStoreFixtures.identity(
                        file,
                        configuration.identityType,
                        configuration.storePassword,
                        configuration.keyPassword,
                        "client",
                        fixture.clientKey,
                        fixture.clientCert);
            client =
                    new KeyStoreSettings(
                            configuration.identityType, file, configuration.storePassword);
        }
        KeyStoreSettings trust = null;
        if (configuration.trustType != null) {
            Path file = dir.resolve("trust.store");
            if (configuration.noMacTrust) {
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        file, null, fixture.serverCert, "trusted", "", null);
                KeyStoreTest.assertUnencryptedSafeContents(file, false, false);
            } else
                KeyStoreFixtures.trust(
                        file,
                        configuration.trustType,
                        configuration.trustPassword,
                        fixture.serverCert);
            trust =
                    new KeyStoreSettings(
                            configuration.trustType, file, configuration.trustPassword);
        }
        return base(loginPassword, configuration.keyPassword, configuration.tokenOnly)
                .withKeyStores(client, null, trust);
    }

    private OpcSettings base(String loginPassword, String keyPassword, boolean tokenOnly) {
        return new OpcSettings(
                fixture.endpoint,
                null,
                tokenOnly ? null : dir.resolve("client.der"),
                tokenOnly ? null : dir.resolve("client.pem"),
                keyPassword,
                dir.resolve("pki"),
                "user",
                loginPassword,
                100,
                5000,
                Collections.singletonList(TagDefinition.parse("time", "ns=0;i=2258", 100, 10)),
                tokenOnly
                        ? new OpcSecurity("None", "None", OpcSecurity.USERNAME)
                        : OpcSecurity.DEFAULT);
    }

    private static OpcConnection connect(
            OpcSettings settings, ConnectionDiagnostics report, BlockingQueue<DataValue> values)
            throws Exception {
        return new OpcConnection(
                settings,
                (tag, value) -> values.offer(value),
                () -> {},
                () -> {},
                new ConnectionAttempt(),
                report);
    }

    @AfterEach
    void close() throws Exception {
        if (fixture != null) fixture.closeServer();
    }
}
