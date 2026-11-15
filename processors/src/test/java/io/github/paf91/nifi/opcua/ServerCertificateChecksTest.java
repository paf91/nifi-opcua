/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.cert.jcajce.*;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.nio.file.*;
import java.security.*;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

class ServerCertificateChecksTest {
    @TempDir Path dir;

    static Stream<Object[]> configurations() {
        return ServerCertificateValidationTest.configurations()
                .filter(c -> !(boolean) c[2])
                .filter(
                        c ->
                                c[1] != ServerCertificateValidationTest.Scenario.MISSING_PKI
                                        && c[1]
                                                != ServerCertificateValidationTest.Scenario
                                                        .PKI_IS_FILE);
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("configurations")
    void validatesRealCertificatesWithoutANetwork(
            ServerCertificateValidation mode,
            ServerCertificateValidationTest.Scenario scenario,
            boolean ignored)
            throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        X509Certificate certificate =
                ServerCertificateValidationTest.certificate(scenario, generator.generateKeyPair());
        Path pki = dir.resolve("pki");
        if (scenario != ServerCertificateValidationTest.Scenario.UNTRUSTED) {
            Files.createDirectories(pki.resolve("trusted/certs"));
            Files.write(pki.resolve("trusted/certs/server.der"), certificate.getEncoded());
        }
        boolean accepted =
                mode == ServerCertificateValidation.NONE
                        || scenario == ServerCertificateValidationTest.Scenario.VALID
                        || (mode == ServerCertificateValidation.CERTIFICATE
                                && scenario == ServerCertificateValidationTest.Scenario.HOSTNAME);
        org.junit.jupiter.api.function.Executable validate =
                () ->
                        TestServerTrust.validate(
                                mode,
                                pki,
                                Collections.singletonList(certificate),
                                "urn:test:server",
                                "localhost");
        if (accepted) assertDoesNotThrow(validate);
        else {
            UaException failure = assertThrows(UaException.class, validate);
            switch (scenario) {
                case HOSTNAME:
                    assertEquals(
                            StatusCodes.Bad_CertificateHostNameInvalid,
                            failure.getStatusCode().getValue());
                    break;
                case URI:
                    assertEquals(
                            StatusCodes.Bad_CertificateUriInvalid,
                            failure.getStatusCode().getValue());
                    break;
                case EXPIRED:
                    assertEquals(
                            StatusCodes.Bad_CertificateTimeInvalid,
                            failure.getStatusCode().getValue());
                    break;
                case KEY_USAGE:
                    assertEquals(
                            StatusCodes.Bad_CertificateUseNotAllowed,
                            failure.getStatusCode().getValue());
                    break;
                default:
                    break;
            }
        }
    }

    static Stream<Object[]> caConfigurations() {
        List<Object[]> cases = new ArrayList<>();
        for (ServerCertificateValidation mode : ServerCertificateValidation.values())
            for (String revocation : Arrays.asList("valid", "revoked", "missing"))
                for (String source : Arrays.asList("PKI Directory", "JKS", "PKCS12"))
                    cases.add(new Object[] {mode, revocation, source});
        return cases.stream();
    }

    @ParameterizedTest(name = "CA chain {0} / CRL {1} / {2}")
    @MethodSource("caConfigurations")
    void certificateModeRetainsCaTrustAndRevocationChecks(
            ServerCertificateValidation mode, String revocation, String source) throws Exception {
        CaIdentity ca = CaIdentity.INSTANCE;
        Path pki = dir.resolve("ca-pki");
        Files.createDirectories(pki.resolve("trusted/certs"));
        KeyStoreSettings store = null;
        if ("PKI Directory".equals(source))
            Files.write(pki.resolve("trusted/certs/ca.der"), ca.root.getEncoded());
        else {
            Path file =
                    KeyStoreFixtures.trust(
                            dir.resolve("trust.store"), source, "store-secret", ca.root);
            store = new KeyStoreSettings(source, file, "store-secret");
            Files.write(
                    pki.resolve("trusted/certs/ignored.der"),
                    KeyStoreFixtures.OTHER.certificate.getEncoded());
        }
        writeIssuersAndCrls(pki, revocation);
        KeyStoreSettings selectedStore = store;
        org.junit.jupiter.api.function.Executable validate =
                () ->
                        TestServerTrust.validate(
                                mode,
                                pki,
                                selectedStore,
                                Collections.singletonList(ca.leaf),
                                "urn:test:server",
                                "localhost");
        if (mode == ServerCertificateValidation.NONE || "valid".equals(revocation))
            assertDoesNotThrow(validate);
        else {
            UaException failure = assertThrows(UaException.class, validate);
            assertEquals(
                    "revoked".equals(revocation)
                            ? StatusCodes.Bad_CertificateRevoked
                            : StatusCodes.Bad_CertificateRevocationUnknown,
                    failure.getStatusCode().getValue());
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"JKS", "PKCS12"})
    void storeTrustNeverImportsPkiTrustedCertificates(String source) throws Exception {
        CaIdentity ca = CaIdentity.INSTANCE;
        Path pki = dir.resolve("pki");
        Files.createDirectories(pki.resolve("trusted/certs"));
        Files.write(pki.resolve("trusted/certs/real-root.der"), ca.root.getEncoded());
        writeIssuersAndCrls(pki, "valid");
        Path file =
                KeyStoreFixtures.trust(
                        dir.resolve("unrelated.store"),
                        source,
                        "store-secret",
                        KeyStoreFixtures.OTHER.certificate);
        byte[] original = Files.readAllBytes(file);
        for (ServerCertificateValidation mode :
                Arrays.asList(
                        ServerCertificateValidation.FULL, ServerCertificateValidation.CERTIFICATE))
            assertThrows(
                    UaException.class,
                    () ->
                            TestServerTrust.validate(
                                    mode,
                                    pki,
                                    new KeyStoreSettings(source, file, "store-secret"),
                                    Collections.singletonList(ca.leaf),
                                    "urn:test:server",
                                    "localhost"));
        assertArrayEquals(original, Files.readAllBytes(file));
    }

    @Test
    void trustStoreSnapshotsAreReadOnlyAndNewManagersReloadTheFile() throws Exception {
        Path file =
                KeyStoreFixtures.trust(
                        dir.resolve("trust.jks"),
                        "JKS",
                        "store-secret",
                        KeyStoreFixtures.CLIENT.certificate);
        KeyStoreSettings settings = new KeyStoreSettings("JKS", file, "store-secret");
        try (StoreTrustListManager first = new StoreTrustListManager(settings, null)) {
            assertEquals(
                    Collections.singletonList(KeyStoreFixtures.CLIENT.certificate),
                    first.getTrustedCertificates());
            byte[] before = Files.readAllBytes(file);
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> first.addTrustedCertificate(KeyStoreFixtures.OTHER.certificate));
            assertArrayEquals(before, Files.readAllBytes(file));
            KeyStoreFixtures.trust(file, "JKS", "store-secret", KeyStoreFixtures.OTHER.certificate);
            assertEquals(
                    Collections.singletonList(KeyStoreFixtures.CLIENT.certificate),
                    first.getTrustedCertificates());
            try (StoreTrustListManager second = new StoreTrustListManager(settings, null)) {
                assertEquals(
                        Collections.singletonList(KeyStoreFixtures.OTHER.certificate),
                        second.getTrustedCertificates());
                assertTrue(second.getIssuerCertificates().isEmpty());
                assertTrue(second.getTrustedCrls().isEmpty());
                assertTrue(second.getIssuerCrls().isEmpty());
            }
        }
    }

    private static void writeIssuersAndCrls(Path pki, String revocation) throws Exception {
        CaIdentity ca = CaIdentity.INSTANCE;
        Path issuers = pki.resolve(TestServerTrust.issuerDirectory());
        Files.createDirectories(issuers.resolve("certs"));
        Files.write(issuers.resolve("certs/intermediate.der"), ca.intermediate.getEncoded());
        Files.createDirectories(pki.resolve("trusted/crl"));
        Files.write(pki.resolve("trusted/crl/root.crl"), ca.rootCrl);
        if (!"missing".equals(revocation)) {
            Files.createDirectories(issuers.resolve("crl"));
            Files.write(
                    issuers.resolve("crl/intermediate.crl"),
                    "revoked".equals(revocation) ? ca.revokedCrl : ca.validCrl);
        }
    }

    private static final class CaIdentity {
        static final CaIdentity INSTANCE = create();
        final X509Certificate root;
        final X509Certificate intermediate;
        final X509Certificate leaf;
        final byte[] rootCrl;
        final byte[] validCrl;
        final byte[] revokedCrl;

        CaIdentity(
                X509Certificate root,
                X509Certificate intermediate,
                X509Certificate leaf,
                byte[] rootCrl,
                byte[] validCrl,
                byte[] revokedCrl) {
            this.root = root;
            this.intermediate = intermediate;
            this.leaf = leaf;
            this.rootCrl = rootCrl;
            this.validCrl = validCrl;
            this.revokedCrl = revokedCrl;
        }

        private static CaIdentity create() {
            try {
                Security.addProvider(new BouncyCastleProvider());
                KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
                generator.initialize(2048);
                KeyPair rootKey = generator.generateKeyPair();
                KeyPair intermediateKey = generator.generateKeyPair();
                KeyPair leafKey = generator.generateKeyPair();
                X500Name rootName = new X500Name("CN=Test root CA,C=US");
                X500Name intermediateName = new X500Name("CN=Test intermediate CA,C=US");
                Instant now = Instant.now();
                Date before = Date.from(now.minusSeconds(3600));
                Date after = Date.from(now.plusSeconds(86400));
                org.bouncycastle.operator.ContentSigner rootSigner =
                        new JcaContentSignerBuilder("SHA256withRSA")
                                .setProvider("BC")
                                .build(rootKey.getPrivate());
                org.bouncycastle.operator.ContentSigner intermediateSigner =
                        new JcaContentSignerBuilder("SHA256withRSA")
                                .setProvider("BC")
                                .build(intermediateKey.getPrivate());
                X509Certificate root =
                        certificate(
                                rootName, rootName, rootKey, 1, true, before, after, rootSigner);
                X509Certificate intermediate =
                        certificate(
                                rootName,
                                intermediateName,
                                intermediateKey,
                                2,
                                true,
                                before,
                                after,
                                rootSigner);
                X509Certificate leaf =
                        certificate(
                                intermediateName,
                                new X500Name("CN=Test server,C=US"),
                                leafKey,
                                3,
                                false,
                                before,
                                after,
                                intermediateSigner);
                X509v2CRLBuilder rootCrl = new X509v2CRLBuilder(rootName, before);
                rootCrl.setNextUpdate(after);
                X509v2CRLBuilder intermediateCrl = new X509v2CRLBuilder(intermediateName, before);
                intermediateCrl.setNextUpdate(after);
                byte[] valid = intermediateCrl.build(intermediateSigner).getEncoded();
                intermediateCrl.addCRLEntry(BigInteger.valueOf(3), before, CRLReason.keyCompromise);
                return new CaIdentity(
                        root,
                        intermediate,
                        leaf,
                        rootCrl.build(rootSigner).getEncoded(),
                        valid,
                        intermediateCrl.build(intermediateSigner).getEncoded());
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private static X509Certificate certificate(
                X500Name issuer,
                X500Name subject,
                KeyPair key,
                int serial,
                boolean ca,
                Date before,
                Date after,
                org.bouncycastle.operator.ContentSigner signer)
                throws Exception {
            JcaX509v3CertificateBuilder builder =
                    new JcaX509v3CertificateBuilder(
                            issuer,
                            BigInteger.valueOf(serial),
                            before,
                            after,
                            subject,
                            key.getPublic());
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
            builder.addExtension(
                    Extension.keyUsage,
                    true,
                    new KeyUsage(
                            ca
                                    ? KeyUsage.keyCertSign | KeyUsage.cRLSign
                                    : KeyUsage.digitalSignature
                                            | KeyUsage.nonRepudiation
                                            | KeyUsage.keyEncipherment
                                            | KeyUsage.dataEncipherment));
            if (!ca) {
                builder.addExtension(
                        Extension.extendedKeyUsage,
                        false,
                        new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth));
                builder.addExtension(
                        Extension.subjectAlternativeName,
                        false,
                        new GeneralNames(
                                new GeneralName[] {
                                    new GeneralName(
                                            GeneralName.uniformResourceIdentifier,
                                            "urn:test:server"),
                                    new GeneralName(GeneralName.dNSName, "localhost")
                                }));
            }
            return new JcaX509CertificateConverter()
                    .setProvider("BC")
                    .getCertificate(builder.build(signer));
        }
    }
}
