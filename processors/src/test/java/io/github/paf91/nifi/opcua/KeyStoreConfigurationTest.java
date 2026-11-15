/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.*;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.*;

class KeyStoreConfigurationTest {
    @TempDir Path dir;

    @Test
    void defaultsKeepPemIdentityPkiTrustAndIndependentEmptyPasswords() {
        TestRunner runner = configured();
        try {
            runner.assertValid();
            assertEquals("PEM/DER", value(runner, ConsumeOPCUA.CLIENT_IDENTITY_FORMAT));
            assertEquals("PKI Directory", value(runner, ConsumeOPCUA.SERVER_TRUST_SOURCE));
            assertNull(value(runner, ConsumeOPCUA.KEYSTORE_PASSWORD));
            assertNull(value(runner, ConsumeOPCUA.TRUSTSTORE_PASSWORD));
            assertNull(value(runner, ConsumeOPCUA.KEY_PASSWORD));
            assertNull(value(runner, ConsumeOPCUA.KEY_ALIAS));
        } finally {
            runner.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void identityStoreRequiresItsFileAndRejectsLegacyIdentityPaths(String type) {
        TestRunner runner = configured();
        try {
            runner.setProperty(ConsumeOPCUA.CLIENT_IDENTITY_FORMAT, type);
            runner.setProperty(ConsumeOPCUA.KEYSTORE, dir.resolve("client.store").toString());
            runner.assertNotValid();
            runner.removeProperty(ConsumeOPCUA.CERTIFICATE);
            runner.assertNotValid();
            runner.removeProperty(ConsumeOPCUA.PRIVATE_KEY);
            runner.assertValid();
            runner.setProperty(ConsumeOPCUA.KEYSTORE_PASSWORD, " store ");
            runner.setProperty(ConsumeOPCUA.KEY_PASSWORD, " key ");
            runner.setProperty(ConsumeOPCUA.KEY_ALIAS, "client");
            runner.assertValid();
            runner.removeProperty(ConsumeOPCUA.KEYSTORE);
            runner.assertNotValid();
        } finally {
            runner.shutdown();
        }
    }

    @Test
    void pemIdentityRejectsEachConflictingStoreProperty() {
        TestRunner runner = configured();
        try {
            for (PropertyDescriptor property :
                    Arrays.asList(
                            ConsumeOPCUA.KEYSTORE,
                            ConsumeOPCUA.KEYSTORE_PASSWORD,
                            ConsumeOPCUA.KEY_ALIAS)) {
                runner.setProperty(
                        property,
                        property == ConsumeOPCUA.KEYSTORE
                                ? dir.resolve("unused.store").toString()
                                : "configured");
                runner.assertNotValid();
                runner.removeProperty(property);
                runner.assertValid();
            }
        } finally {
            runner.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void trustStoreNeedsItsFileAndCanUseAnOptionalPkiDirectory(String type) {
        TestRunner runner = configured();
        try {
            runner.setProperty(ConsumeOPCUA.SERVER_TRUST_SOURCE, type);
            runner.assertNotValid();
            runner.setProperty(ConsumeOPCUA.TRUSTSTORE, dir.resolve("trust.store").toString());
            runner.assertValid();
            runner.removeProperty(ConsumeOPCUA.PKI);
            runner.assertValid();
            runner.setProperty(ConsumeOPCUA.TRUSTSTORE_PASSWORD, " trust ");
            runner.assertValid();
            runner.removeProperty(ConsumeOPCUA.TRUSTSTORE);
            runner.assertNotValid();
        } finally {
            runner.shutdown();
        }
    }

    @Test
    void pkiTrustRejectsEachStorePropertyUnlessValidationIsDisabled() {
        TestRunner runner = configured();
        try {
            for (PropertyDescriptor property :
                    Arrays.asList(ConsumeOPCUA.TRUSTSTORE, ConsumeOPCUA.TRUSTSTORE_PASSWORD)) {
                runner.setProperty(
                        property,
                        property == ConsumeOPCUA.TRUSTSTORE
                                ? dir.resolve("unused.store").toString()
                                : "configured");
                runner.assertNotValid();
                runner.setProperty(ConsumeOPCUA.SERVER_CERTIFICATE_VALIDATION, "None");
                runner.assertValid();
                runner.setProperty(ConsumeOPCUA.SERVER_CERTIFICATE_VALIDATION, "Full");
                runner.removeProperty(property);
            }
            runner.setProperty(ConsumeOPCUA.SERVER_TRUST_SOURCE, "JKS");
            runner.setProperty(ConsumeOPCUA.SERVER_CERTIFICATE_VALIDATION, "None");
            runner.removeProperty(ConsumeOPCUA.PKI);
            runner.assertValid();
        } finally {
            runner.shutdown();
        }
    }

    @Test
    void noneChannelDoesNotLoadUnusedIdentityFilesButStillValidatesAbsolutePaths()
            throws Exception {
        TestRunner runner = configured();
        try {
            runner.setProperty(ConsumeOPCUA.SECURITY_POLICY, "None");
            runner.setProperty(ConsumeOPCUA.SECURITY_MODE, "None");
            runner.setProperty(ConsumeOPCUA.AUTHENTICATION, OpcSecurity.ANONYMOUS);
            runner.setProperty(ConsumeOPCUA.CLIENT_IDENTITY_FORMAT, "JKS");
            runner.setProperty(ConsumeOPCUA.KEYSTORE, dir.resolve("missing.store").toString());
            runner.setProperty(ConsumeOPCUA.KEYSTORE_PASSWORD, "unused-secret");
            runner.assertValid();
            OpcSettings settings =
                    base(new OpcSecurity("None", "None", OpcSecurity.ANONYMOUS))
                            .withKeyStores(
                                    new KeyStoreSettings(
                                            "JKS", dir.resolve("missing.store"), "unused-secret"),
                                    "missing",
                                    null);
            assertNull(ClientIdentity.load(settings));
            runner.setProperty(ConsumeOPCUA.KEYSTORE, "relative.store");
            runner.assertNotValid();
        } finally {
            runner.shutdown();
        }
    }

    @Test
    void newDescriptorsDisableExpressionLanguageAndProtectOnlyPasswords() {
        for (PropertyDescriptor property :
                Arrays.asList(
                        ConsumeOPCUA.CLIENT_IDENTITY_FORMAT,
                        ConsumeOPCUA.KEYSTORE,
                        ConsumeOPCUA.KEYSTORE_PASSWORD,
                        ConsumeOPCUA.KEY_ALIAS,
                        ConsumeOPCUA.SERVER_TRUST_SOURCE,
                        ConsumeOPCUA.TRUSTSTORE,
                        ConsumeOPCUA.TRUSTSTORE_PASSWORD)) {
            assertEquals(ExpressionLanguageScope.NONE, property.getExpressionLanguageScope());
            assertEquals(
                    property == ConsumeOPCUA.KEYSTORE_PASSWORD
                            || property == ConsumeOPCUA.TRUSTSTORE_PASSWORD,
                    property.isSensitive());
        }
    }

    @Test
    void outputAndStoreCopiesPreserveEveryStoreSetting() {
        KeyStoreSettings client = new KeyStoreSettings("JKS", dir.resolve("client.jks"), " store ");
        KeyStoreSettings trust =
                new KeyStoreSettings("PKCS12", dir.resolve("trust.p12"), " trust ");
        OutputSettings output =
                new OutputSettings("tag,value", "NDJSON", "Epoch Milliseconds", true, 5);
        OpcSettings original = base(OpcSecurity.DEFAULT);
        OpcSettings first = original.withKeyStores(client, "client", trust).withOutput(output);
        OpcSettings second = original.withOutput(output).withKeyStores(client, "client", trust);
        for (OpcSettings settings : Arrays.asList(first, second)) {
            assertSame(client, settings.clientKeyStore());
            assertEquals("client", settings.clientKeyAlias());
            assertSame(trust, settings.serverTrustStore());
            assertSame(output, settings.output());
            assertEquals("key-secret", settings.privateKeyPassword());
            assertEquals(original.endpoint(), settings.endpoint());
            assertSame(original.tags(), settings.tags());
        }
        assertNull(original.clientKeyStore());
        assertNull(original.serverTrustStore());
        assertSame(OutputSettings.DEFAULT, original.output());
        assertEquals("", new KeyStoreSettings("PKCS12", null, null).password());
    }

    @Test
    void verificationPassesExactPasswordsAndEmptyKeyPasswordWithoutStoreFallback()
            throws Exception {
        Capturing processor = new Capturing();
        TestRunner runner = ProcessorTest.configured(processor);
        try {
            runner.setProperty(ConsumeOPCUA.CLIENT_IDENTITY_FORMAT, "JKS");
            runner.removeProperty(ConsumeOPCUA.CERTIFICATE);
            runner.removeProperty(ConsumeOPCUA.PRIVATE_KEY);
            runner.setProperty(ConsumeOPCUA.KEYSTORE, dir.resolve("client.jks").toString());
            runner.setProperty(ConsumeOPCUA.KEYSTORE_PASSWORD, " store ");
            runner.setProperty(ConsumeOPCUA.KEY_ALIAS, "client");
            runner.setProperty(ConsumeOPCUA.SERVER_TRUST_SOURCE, "PKCS12");
            runner.setProperty(ConsumeOPCUA.TRUSTSTORE, dir.resolve("trust.p12").toString());
            runner.setProperty(ConsumeOPCUA.TRUSTSTORE_PASSWORD, " trust ");
            runner.assertValid();
            ConfigVerificationResult result =
                    processor
                            .verify(
                                    runner.getProcessContext(),
                                    runner.getLogger(),
                                    Collections.emptyMap())
                            .get(0);
            assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome());
            assertEquals(" store ", processor.settings.clientKeyStore().password());
            assertEquals(" trust ", processor.settings.serverTrustStore().password());
            assertEquals("", processor.settings.privateKeyPassword());
            assertEquals("client", processor.settings.clientKeyAlias());
            assertEquals(1, processor.closed);
            assertTrue(processor.attempt.isCancelled());
        } finally {
            runner.shutdown();
        }
    }

    private static final class Capturing extends ConsumeOPCUA {
        OpcSettings settings;
        ConnectionAttempt attempt;
        int closed;

        @Override
        protected AutoCloseable verifyConnection(
                OpcSettings settings, ConnectionAttempt attempt, ConnectionDiagnostics report) {
            this.settings = settings;
            this.attempt = attempt;
            report.stage("Subscribed");
            return () -> closed++;
        }
    }

    private static TestRunner configured() {
        return ProcessorTest.configured(new ProcessorTest.OfflineProcessor());
    }

    private static String value(TestRunner runner, PropertyDescriptor property) {
        return runner.getProcessContext().getProperty(property).getValue();
    }

    private OpcSettings base(OpcSecurity security) {
        return new OpcSettings(
                "opc.tcp://localhost:4840",
                null,
                dir.resolve("missing.der"),
                dir.resolve("missing.key"),
                "key-secret",
                dir.resolve("pki"),
                "user",
                "test-password",
                100,
                1000,
                Collections.emptyList(),
                security);
    }
}
