/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.util.TestRunner;
import org.eclipse.milo.opcua.stack.core.security.TrustListManager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.cert.CertPathBuilderException;
import java.security.cert.X509Certificate;
import java.util.*;

class TrustVerificationTest {
    @TempDir Path dir;
    private final String issuer = TestServerTrust.issuerDirectory();
    private final ConnectionDiagnostics report = new ConnectionDiagnostics(value -> value, true);

    private OpcSettings settings(Path pki) {
        return new OpcSettings(
                "opc.tcp://localhost:4840",
                null,
                null,
                null,
                null,
                pki,
                null,
                null,
                100,
                1000,
                Collections.emptyList());
    }

    private static byte[] bundle(X509Certificate... certs) throws Exception {
        StringBuilder pem = new StringBuilder();
        Base64.Encoder encoder = Base64.getMimeEncoder(64, new byte[] {'\n'});
        for (X509Certificate cert : certs) {
            pem.append("-----BEGIN CERTIFICATE-----\n")
                    .append(encoder.encodeToString(cert.getEncoded()))
                    .append("\n-----END CERTIFICATE-----\n");
        }
        return pem.toString().getBytes(StandardCharsets.US_ASCII);
    }

    @Test
    void verifyShowsTrustAndFingerprintBeforeDetailedEndpointMetadataOnFailure() {
        ConsumeOPCUA processor =
                new ConsumeOPCUA() {
                    @Override
                    protected AutoCloseable verifyConnection(
                            OpcSettings settings,
                            ConnectionAttempt generation,
                            ConnectionDiagnostics diagnostics)
                            throws Exception {
                        diagnostics.endpoints(Collections.emptyList());
                        diagnostics.certificate(
                                "serverCertificate", KeyStoreFixtures.OTHER.certificate);
                        diagnostics.trustRequired(ServerCertificateValidation.FULL, true);
                        try (AutoCloseable manager = TrustDirectory.open(settings.pki())) {
                            diagnostics.trustLoaded(settings, (TrustListManager) manager, issuer);
                        }
                        throw new CertPathBuilderException("No trusted path");
                    }
                };
        TestRunner runner = ProcessorTest.configured(processor);
        runner.setProperty(ConsumeOPCUA.PKI, dir.toString());
        ConfigVerificationResult result =
                processor.verify(runner.getProcessContext(), null, Collections.emptyMap()).get(0);
        assertEquals(ConfigVerificationResult.Outcome.FAILED, result.getOutcome());
        String text = result.getExplanation();
        assertTrue(text.contains("No trusted path"), text);
        assertTrue(text.contains("trusted certificates = 0"), text);
        assertTrue(
                text.matches(
                        "(?s).*Advertised server leaf SHA-256 ="
                                + " [0-9a-f]{64}\\..*advertisedEndpoints.*"),
                text);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void reportsWhatTheDirectoryManagerActuallyLoaded(boolean reverse) throws Exception {
        X509Certificate first =
                reverse ? KeyStoreFixtures.OTHER.certificate : KeyStoreFixtures.CLIENT.certificate;
        X509Certificate second =
                reverse ? KeyStoreFixtures.CLIENT.certificate : KeyStoreFixtures.OTHER.certificate;
        TrustDirectory.prepare(dir);
        Files.write(dir.resolve("trusted/certs/factory-bundle.pem"), bundle(first, second));
        Files.write(dir.resolve(issuer + "/certs/second.der"), second.getEncoded());
        try (AutoCloseable resource = TrustDirectory.open(dir)) {
            TrustListManager manager = (TrustListManager) resource;
            assertEquals(Collections.singletonList(first), manager.getTrustedCertificates());
            report.trustLoaded(settings(dir), manager, issuer);
        }
        String explanation = report.trustExplanation(new Exception(new CertPathBuilderException()));
        assertTrue(explanation.contains("PKI Directory"), explanation);
        assertTrue(
                explanation.contains("trusted certificates = 1, issuer certificates = 1"),
                explanation);
        assertTrue(explanation.contains("multiple PEM"), explanation);
        assertTrue(explanation.contains(issuer + "/certs"), explanation);
        assertTrue(explanation.contains("fingerprint"), explanation);
        assertFalse(explanation.contains("factory-bundle"));
        assertFalse(explanation.contains(dir.toString()));
        assertTrue(
                report.trustExplanation(
                                new java.security.InvalidAlgorithmParameterException(
                                        "the trustAnchors parameter must be non-empty"))
                        .contains("put its issuing root"));
        assertFalse(
                report.trustExplanation(new Exception("remote service fault"))
                        .contains("fingerprint"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void checksOnlyTheIssuerSupplementWhenTrustComesFromAStore(String type) throws Exception {
        X509Certificate cert = KeyStoreFixtures.CLIENT.certificate;
        X509Certificate other = KeyStoreFixtures.OTHER.certificate;
        Path storePath =
                KeyStoreFixtures.trust(
                        dir.resolve("private-store"), type, "store-secret", cert, other);
        KeyStoreSettings store = new KeyStoreSettings(type, storePath, "store-secret");
        Path pki = dir.resolve("pki");
        TrustDirectory.prepare(pki);
        Files.write(pki.resolve("trusted/certs/ignored.pem"), bundle(cert, other));
        OpcSettings settings = settings(pki).withKeyStores(null, null, store);
        try (StoreTrustListManager manager = new StoreTrustListManager(store, pki)) {
            report.trustLoaded(settings, manager, issuer);
            String explanation = report.trustExplanation(new CertPathBuilderException());
            assertTrue(
                    explanation.contains("trusted certificates = 2, issuer certificates = 0"),
                    explanation);
            assertTrue(explanation.contains("selected " + type + " store"), explanation);
            assertFalse(explanation.contains("multiple PEM"));
            assertFalse(explanation.contains("private-store"));
            assertFalse(explanation.contains("store-secret"));
            Files.write(pki.resolve(issuer + "/certs/bundle.pem"), bundle(cert, other));
            report.trustLoaded(settings, manager, issuer);
            assertTrue(report.trustExplanation(null).contains("multiple PEM"));
        }
        try (StoreTrustListManager manager = new StoreTrustListManager(store, null)) {
            report.trustLoaded(settings(null).withKeyStores(null, null, store), manager, issuer);
            assertTrue(
                    report.trustExplanation(new CertPathBuilderException())
                            .contains("configure a PKI Directory"));
            assertFalse(report.trustExplanation(null).contains("multiple PEM"));
        }
    }

    @Test
    void distinguishesAnUnopenedOrUnusedTrustSourceFromAnEmptyManager() throws Exception {
        report.trustSource("PKCS12");
        assertTrue(report.trustExplanation(null).contains("not reached"));
        report.trustRequired(ServerCertificateValidation.FULL, true);
        assertTrue(report.trustExplanation(null).contains("could not be opened"));
        report.trustRequired(ServerCertificateValidation.CERTIFICATE, false);
        assertTrue(report.trustExplanation(null).contains("do not require a server certificate"));
        report.trustRequired(ServerCertificateValidation.NONE, true);
        assertTrue(report.trustExplanation(null).contains("skipped"));
        try (AutoCloseable resource = TrustDirectory.open(dir)) {
            report.trustLoaded(settings(dir), (TrustListManager) resource, issuer);
        }
        assertTrue(report.trustExplanation(null).contains("trusted certificates = 0"));
        ConnectionDiagnostics running = new ConnectionDiagnostics();
        running.trustLoaded(null, null, null);
        assertEquals("", running.trustExplanation(null));
    }

    @Test
    void reportsAnIncompleteAdvisoryForLargeFilesAndDirectoryLimits() throws Exception {
        Path certs = Files.createDirectories(dir.resolve("trusted/certs"));
        Files.createDirectories(dir.resolve(issuer + "/certs"));
        Path large = certs.resolve("large.pem");
        Files.write(large, new byte[64 * 1024]);
        assertEquals("", PemBundleCheck.inspect(dir, true, issuer + "/certs"));
        Files.write(
                large,
                bundle(KeyStoreFixtures.CLIENT.certificate, KeyStoreFixtures.OTHER.certificate),
                StandardOpenOption.APPEND);
        String truncated = PemBundleCheck.inspect(dir, true, issuer + "/certs");
        assertTrue(truncated.contains("part of"), truncated);
        assertFalse(truncated.contains("multiple PEM"));
        Files.delete(large);
        for (int i = 0; i < 129; i++) Files.createFile(certs.resolve("empty-" + i));
        assertTrue(PemBundleCheck.inspect(dir, true, issuer + "/certs").contains("part of"));
    }

    @Test
    void checksTheFullSharedByteBudgetAcrossTrustDirectories() throws Exception {
        Path trusted = Files.createDirectories(dir.resolve("trusted/certs"));
        Path issuers = Files.createDirectories(dir.resolve(issuer + "/certs"));
        byte[] block = new byte[32 * 1024];
        for (int i = 0; i < 15; i++) Files.write(trusted.resolve("cert-" + i), block);
        Path last = issuers.resolve("last.pem");
        byte[] pem =
                bundle(KeyStoreFixtures.CLIENT.certificate, KeyStoreFixtures.OTHER.certificate);
        Files.write(last, new byte[block.length - pem.length]);
        Files.write(last, pem, StandardOpenOption.APPEND);
        String complete = PemBundleCheck.inspect(dir, true, issuer + "/certs");
        assertTrue(complete.contains("multiple PEM"), complete);
        assertFalse(complete.contains("part of"), complete);

        Files.write(last, new byte[] {0}, StandardOpenOption.APPEND);
        String truncated = PemBundleCheck.inspect(dir, true, issuer + "/certs");
        assertTrue(truncated.contains("part of"), truncated);
    }

    @Test
    void skipsLinkedFilesAndReportsThePartialCheck() throws Exception {
        Path certs = Files.createDirectories(dir.resolve("trusted/certs"));
        Files.createDirectories(dir.resolve(issuer + "/certs"));
        Path external = dir.resolve("external.pem");
        Files.write(
                external,
                bundle(KeyStoreFixtures.CLIENT.certificate, KeyStoreFixtures.OTHER.certificate));
        try {
            Files.createSymbolicLink(certs.resolve("link.pem"), external);
        } catch (java.io.IOException
                | UnsupportedOperationException
                | SecurityException unavailable) {
            Assumptions.assumeTrue(false, "Symbolic links unavailable");
        }
        String text = PemBundleCheck.inspect(dir, true, issuer + "/certs");
        assertTrue(text.contains("part of"), text);
        assertFalse(text.contains("multiple PEM"));
    }
}
