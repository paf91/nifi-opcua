/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.bouncycastle.asn1.*;
import org.bouncycastle.asn1.pkcs.*;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.io.InputStream;
import java.nio.file.*;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.stream.Stream;

import javax.crypto.spec.SecretKeySpec;

class KeyStoreTest {
    @TempDir Path dir;

    static Stream<Object[]> passwordCombinations() {
        List<Object[]> cases = new ArrayList<>();
        for (String type : Arrays.asList("JKS", "PKCS12"))
            for (String storePassword : Arrays.asList("", "store-secret"))
                for (String keyPassword : Arrays.asList("", "key-secret"))
                    cases.add(new Object[] {type, storePassword, keyPassword});
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}, store password={1}, key password={2}")
    @MethodSource("passwordCombinations")
    void storeAndPrivateKeyPasswordsAreIndependent(
            String type, String storePassword, String keyPassword) throws Exception {
        Path file = identity(type, storePassword, keyPassword);
        byte[] original = Files.readAllBytes(file);
        char[] suppliedStorePassword = storePassword.toCharArray();
        KeyStore store = KeyStoreLoader.load(file, type, suppliedStorePassword);
        assertArrayEquals(new char[suppliedStorePassword.length], suppliedStorePassword);
        assertTrue(store.isKeyEntry("client"));

        char[] suppliedKeyPassword = keyPassword.toCharArray();
        ClientIdentity loaded =
                ClientIdentity.load(
                        new KeyStoreSettings(type, file, storePassword),
                        null,
                        suppliedKeyPassword,
                        null);
        assertIdentity(loaded, KeyStoreFixtures.CLIENT);
        assertArrayEquals(new char[suppliedKeyPassword.length], suppliedKeyPassword);
        assertArrayEquals(original, Files.readAllBytes(file));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void whitespacePasswordsAreNotTrimmed(String type) throws Exception {
        Path file = identity(type, " store ", " key ");
        char[] keyPassword = " key ".toCharArray();
        assertIdentity(
                ClientIdentity.load(
                        new KeyStoreSettings(type, file, " store "),
                        "client",
                        keyPassword,
                        "urn:test:client"),
                KeyStoreFixtures.CLIENT);
        assertArrayEquals(new char[keyPassword.length], keyPassword);
        assertThrows(
                Exception.class,
                () ->
                        ClientIdentity.load(
                                new KeyStoreSettings(type, file, "store"),
                                "client",
                                " key ".toCharArray(),
                                null));
        assertThrows(
                Exception.class,
                () ->
                        ClientIdentity.load(
                                new KeyStoreSettings(type, file, " store "),
                                "client",
                                "key".toCharArray(),
                                null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void wrongOrOmittedStorePasswordNeverSkipsIntegrityChecking(String type) throws Exception {
        Path file = identity(type, "store-secret", "key-secret");
        for (String wrong : Arrays.asList("wrong", "")) {
            char[] suppliedStorePassword = wrong.toCharArray();
            assertThrows(
                    Exception.class, () -> KeyStoreLoader.load(file, type, suppliedStorePassword));
            assertArrayEquals(new char[suppliedStorePassword.length], suppliedStorePassword);
            char[] suppliedKeyPassword = "key-secret".toCharArray();
            assertThrows(
                    Exception.class,
                    () ->
                            ClientIdentity.load(
                                    new KeyStoreSettings(type, file, wrong),
                                    null,
                                    suppliedKeyPassword,
                                    null));
            assertArrayEquals(new char[suppliedKeyPassword.length], suppliedKeyPassword);
        }
        assertThrows(Exception.class, () -> KeyStoreLoader.load(file, type, null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void wrongOrOmittedKeyPasswordDoesNotFallBackToStorePassword(String type) throws Exception {
        Path file = identity(type, "store-secret", "key-secret");
        for (String wrong : Arrays.asList("wrong", "", "store-secret")) {
            char[] supplied = wrong.toCharArray();
            assertThrows(
                    Exception.class,
                    () ->
                            ClientIdentity.load(
                                    new KeyStoreSettings(type, file, "store-secret"),
                                    "client",
                                    supplied,
                                    null));
            assertArrayEquals(new char[supplied.length], supplied);
        }
        assertThrows(
                Exception.class,
                () ->
                        ClientIdentity.load(
                                new KeyStoreSettings(type, file, "store-secret"),
                                "client",
                                null,
                                null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void selectsOnlyPrivateKeyEntriesAndRequiresAnAliasWhenAmbiguous(String type) throws Exception {
        KeyStore store = KeyStoreFixtures.empty(type);
        store.setCertificateEntry("certificate", KeyStoreFixtures.OTHER.certificate);
        store.setKeyEntry(
                "client",
                KeyStoreFixtures.CLIENT.key.getPrivate(),
                "key-secret".toCharArray(),
                new X509Certificate[] {KeyStoreFixtures.CLIENT.certificate});
        Path file = KeyStoreFixtures.write(dir.resolve("identity"), store, "store-secret");
        assertIdentity(load(file, type, null, "key-secret"), KeyStoreFixtures.CLIENT);
        assertIdentity(load(file, type, "", "key-secret"), KeyStoreFixtures.CLIENT);

        store.setKeyEntry(
                "other",
                KeyStoreFixtures.OTHER.key.getPrivate(),
                "other-key".toCharArray(),
                new X509Certificate[] {KeyStoreFixtures.OTHER.certificate});
        KeyStoreFixtures.write(file, store, "store-secret");
        char[] wrongPassword = "wrong".toCharArray();
        IllegalArgumentException ambiguous =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ClientIdentity.load(
                                        new KeyStoreSettings(type, file, "store-secret"),
                                        null,
                                        wrongPassword,
                                        null));
        assertNotNull(ambiguous.getMessage());
        assertTrue(ambiguous.getMessage().toLowerCase(Locale.ROOT).contains("alias"));
        assertArrayEquals(new char[wrongPassword.length], wrongPassword);
        assertIdentity(load(file, type, "client", "key-secret"), KeyStoreFixtures.CLIENT);
        assertIdentity(load(file, type, "other", "other-key"), KeyStoreFixtures.OTHER);
        for (String alias : Arrays.asList("missing", "certificate")) {
            char[] password = "key-secret".toCharArray();
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            ClientIdentity.load(
                                    new KeyStoreSettings(type, file, "store-secret"),
                                    alias,
                                    password,
                                    null));
            assertArrayEquals(new char[password.length], password);
        }
    }

    @Test
    void secretKeysAreNotClientIdentityCandidates() throws Exception {
        KeyStore store = KeyStoreFixtures.empty("PKCS12");
        store.setEntry(
                "secret",
                new KeyStore.SecretKeyEntry(new SecretKeySpec(new byte[16], "AES")),
                new KeyStore.PasswordProtection("key-secret".toCharArray()));
        Path file = KeyStoreFixtures.write(dir.resolve("secret.p12"), store, "store-secret");
        for (String alias : new String[] {null, "secret"})
            assertThrows(
                    IllegalArgumentException.class,
                    () -> load(file, "PKCS12", alias, "key-secret"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void retainsCertificateUriValidityRsaAndKeyMatchingChecks(String type) throws Exception {
        Path file =
                KeyStoreFixtures.identity(
                        dir.resolve("mismatch"),
                        type,
                        "store-secret",
                        "key-secret",
                        "client",
                        KeyStoreFixtures.OTHER.key,
                        KeyStoreFixtures.CLIENT.certificate);
        assertThrows(IllegalArgumentException.class, () -> load(file, type, null, "key-secret"));
        identity(type, "store-secret", "key-secret");
        char[] password = "key-secret".toCharArray();
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ClientIdentity.load(
                                new KeyStoreSettings(type, dir.resolve("identity"), "store-secret"),
                                "client",
                                password,
                                "urn:wrong:client"));
        assertArrayEquals(new char[password.length], password);

        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(1024);
        KeyPair shortKey = generator.generateKeyPair();
        Path shortFile =
                KeyStoreFixtures.identity(
                        dir.resolve("short-key"),
                        type,
                        "store-secret",
                        "key-secret",
                        "client",
                        shortKey,
                        ServerCertificateValidationTest.certificate(
                                ServerCertificateValidationTest.Scenario.VALID, shortKey));
        assertThrows(
                IllegalArgumentException.class, () -> load(shortFile, type, null, "key-secret"));
        Path expired =
                KeyStoreFixtures.identity(
                        dir.resolve("expired"),
                        type,
                        "store-secret",
                        "key-secret",
                        "client",
                        KeyStoreFixtures.CLIENT.key,
                        ServerCertificateValidationTest.certificate(
                                ServerCertificateValidationTest.Scenario.EXPIRED,
                                KeyStoreFixtures.CLIENT.key));
        assertThrows(
                java.security.cert.CertificateExpiredException.class,
                () -> load(expired, type, null, "key-secret"));
    }

    @Test
    void rejectsPrivateKeyWithoutACertificateChain() throws Exception {
        Path file =
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        dir.resolve("no-chain.p12"),
                        KeyStoreFixtures.CLIENT.key,
                        null,
                        "client",
                        "key-secret",
                        null);
        assertNull(KeyStoreLoader.load(file, "PKCS12", new char[0]).getCertificateChain("client"));
        assertThrows(
                IllegalArgumentException.class,
                () -> load(file, "PKCS12", "client", "key-secret", ""));
    }

    @Test
    void genuineNoMacPkcs12SupportsPlainCertificatesAndAnEmptyPasswordShroudedKey()
            throws Exception {
        Path identity =
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        dir.resolve("no-mac-identity.p12"),
                        KeyStoreFixtures.CLIENT.key,
                        KeyStoreFixtures.CLIENT.certificate,
                        "client",
                        "",
                        null);
        assertUnencryptedSafeContents(identity, false, true);
        assertIdentity(load(identity, "PKCS12", null, "", ""), KeyStoreFixtures.CLIENT);
        Path trust =
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        dir.resolve("no-mac-trust.p12"),
                        null,
                        KeyStoreFixtures.CLIENT.certificate,
                        "trusted",
                        "",
                        null);
        assertUnencryptedSafeContents(trust, false, false);
        assertEquals(
                Collections.singletonList(KeyStoreFixtures.CLIENT.certificate),
                KeyStoreLoader.trustedCertificates(new KeyStoreSettings("PKCS12", trust, "")));
        assertTrue(KeyStoreLoader.load(trust, "PKCS12", null).isCertificateEntry("trusted"));
    }

    @Test
    void plainCertificateBagsWithAMacStillRequireTheStorePassword() throws Exception {
        Path file =
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        dir.resolve("plain-with-mac.p12"),
                        null,
                        KeyStoreFixtures.CLIENT.certificate,
                        "trusted",
                        "",
                        "mac-secret");
        assertUnencryptedSafeContents(file, true, false);
        assertEquals(
                1,
                KeyStoreLoader.trustedCertificates(
                                new KeyStoreSettings("PKCS12", file, "mac-secret"))
                        .size());
        for (String wrong : Arrays.asList("wrong", ""))
            assertThrows(
                    Exception.class,
                    () ->
                            KeyStoreLoader.trustedCertificates(
                                    new KeyStoreSettings("PKCS12", file, wrong)));
        assertThrows(Exception.class, () -> KeyStoreLoader.load(file, "PKCS12", null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void trustsOnlyCertificateEntriesAndLeavesStoreBytesUnchanged(String type) throws Exception {
        KeyStore store = KeyStoreFixtures.empty(type);
        store.setKeyEntry(
                "identity",
                KeyStoreFixtures.OTHER.key.getPrivate(),
                "key-secret".toCharArray(),
                new X509Certificate[] {KeyStoreFixtures.OTHER.certificate});
        Path file = KeyStoreFixtures.write(dir.resolve("trust"), store, "store-secret");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KeyStoreLoader.trustedCertificates(
                                new KeyStoreSettings(type, file, "store-secret")));
        store.setCertificateEntry("trusted", KeyStoreFixtures.CLIENT.certificate);
        KeyStoreFixtures.write(file, store, "store-secret");
        byte[] original = Files.readAllBytes(file);
        assertEquals(
                Collections.singletonList(KeyStoreFixtures.CLIENT.certificate),
                KeyStoreLoader.trustedCertificates(
                        new KeyStoreSettings(type, file, "store-secret")));
        assertArrayEquals(original, Files.readAllBytes(file));
        KeyStoreFixtures.write(file, KeyStoreFixtures.empty(type), "store-secret");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KeyStoreLoader.trustedCertificates(
                                new KeyStoreSettings(type, file, "store-secret")));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reportsProviderVisibilityForUnmarkedPkcs12CertificatesWithAndWithoutMac(boolean mac)
            throws Exception {
        String password = mac ? "mac-secret" : "";
        Path file =
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        dir.resolve("unmarked.p12"),
                        null,
                        KeyStoreFixtures.CLIENT.certificate,
                        "unmarked",
                        "",
                        mac ? password : null,
                        false);
        assertUnencryptedSafeContents(file, mac, false);
        KeyStore explicitBc = KeyStore.getInstance("PKCS12", new BouncyCastleProvider());
        try (InputStream in = Files.newInputStream(file)) {
            explicitBc.load(in, password.toCharArray());
        }
        assertEquals(1, explicitBc.size());
        assertEquals(KeyStoreFixtures.CLIENT.certificate, explicitBc.getCertificate("unmarked"));
        KeyStore active = KeyStoreLoader.load(file, "PKCS12", password.toCharArray());
        KeyStoreSettings settings = new KeyStoreSettings("PKCS12", file, password);
        if (active.size() == 0) {
            IllegalArgumentException failure =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> KeyStoreLoader.trustedCertificates(settings));
            assertTrue(failure.getMessage().contains("exposes no entries"));
            assertTrue(
                    failure.getMessage().contains("PKCS12")
                            || failure.getMessage().contains("trusted-certificate markers"));
            assertTrue(failure.getMessage().contains("keytool"));
        } else {
            assertTrue(active.isCertificateEntry("unmarked"));
            assertEquals(
                    Collections.singletonList(KeyStoreFixtures.CLIENT.certificate),
                    KeyStoreLoader.trustedCertificates(settings));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void markedPkcs12CertificateEntriesRemainTrustedWithAndWithoutMac(boolean mac)
            throws Exception {
        String password = mac ? "mac-secret" : "";
        Path file =
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        dir.resolve("marked.p12"),
                        null,
                        KeyStoreFixtures.CLIENT.certificate,
                        "marked",
                        "",
                        mac ? password : null,
                        true);
        KeyStore store = KeyStoreLoader.load(file, "PKCS12", password.toCharArray());
        assertEquals(1, store.size());
        assertTrue(store.isCertificateEntry("marked"));
        assertEquals(
                Collections.singletonList(KeyStoreFixtures.CLIENT.certificate),
                KeyStoreLoader.trustedCertificates(new KeyStoreSettings("PKCS12", file, password)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void distinguishesAnEmptyStoreFromPrivateKeyOnlyTrustAndEarlierPasswordFailures(String type)
            throws Exception {
        Path empty =
                KeyStoreFixtures.write(
                        dir.resolve("empty"), KeyStoreFixtures.empty(type), "store-secret");
        IllegalArgumentException emptyFailure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                KeyStoreLoader.trustedCertificates(
                                        new KeyStoreSettings(type, empty, "store-secret")));
        assertTrue(emptyFailure.getMessage().contains("exposes no entries"));
        assertTrue(emptyFailure.getMessage().contains("keytool"));
        Path keyOnly =
                KeyStoreFixtures.identity(
                        dir.resolve("key-only"),
                        type,
                        "store-secret",
                        "key-secret",
                        "client",
                        KeyStoreFixtures.CLIENT.key,
                        KeyStoreFixtures.CLIENT.certificate);
        assertEquals(1, KeyStoreLoader.load(keyOnly, type, "store-secret".toCharArray()).size());
        IllegalArgumentException keyFailure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                KeyStoreLoader.trustedCertificates(
                                        new KeyStoreSettings(type, keyOnly, "store-secret")));
        assertFalse(keyFailure.getMessage().contains("exposes no entries"));
        assertTrue(keyFailure.getMessage().contains("private-key certificate chains"));
        assertNotEquals(emptyFailure.getMessage(), keyFailure.getMessage());
        for (Path protectedStore : Arrays.asList(empty, keyOnly)) {
            Exception passwordFailure =
                    assertThrows(
                            Exception.class,
                            () ->
                                    KeyStoreLoader.trustedCertificates(
                                            new KeyStoreSettings(type, protectedStore, "wrong")));
            assertFalse(
                    passwordFailure instanceof IllegalArgumentException,
                    "Integrity processing must fail before entry visibility is diagnosed");
        }
    }

    @Test
    void unmarkedCertificateMacFailurePrecedesTheVisibilityDiagnostic() throws Exception {
        Path file =
                KeyStoreFixtures.pkcs12WithUnencryptedSafeContents(
                        dir.resolve("unmarked-with-mac.p12"),
                        null,
                        KeyStoreFixtures.CLIENT.certificate,
                        "unmarked",
                        "",
                        "mac-secret",
                        false);
        for (String wrong : Arrays.asList("", "wrong")) {
            Exception failure =
                    assertThrows(
                            Exception.class,
                            () ->
                                    KeyStoreLoader.trustedCertificates(
                                            new KeyStoreSettings("PKCS12", file, wrong)));
            assertFalse(
                    failure instanceof IllegalArgumentException,
                    "A wrong MAC password must fail before entry visibility is diagnosed");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    void rejectsMislabeledStoreTypesAndMissingFilesAndClearsPasswords(String type)
            throws Exception {
        Path file = identity(type, "store-secret", "key-secret");
        char[] password = "store-secret".toCharArray();
        assertThrows(
                Exception.class,
                () -> KeyStoreLoader.load(file, "JKS".equals(type) ? "PKCS12" : "JKS", password));
        assertArrayEquals(new char[password.length], password);
        Path missing = dir.resolve("missing");
        char[] missingPassword = {'t', 'e', 's', 't'};
        NoSuchFileException failure =
                assertThrows(
                        NoSuchFileException.class,
                        () -> KeyStoreLoader.load(missing, type, missingPassword));
        assertEquals(missing.toString(), failure.getFile());
        assertArrayEquals(new char[missingPassword.length], missingPassword);
    }

    private Path identity(String type, String storePassword, String keyPassword) throws Exception {
        return KeyStoreFixtures.identity(
                dir.resolve("identity"),
                type,
                storePassword,
                keyPassword,
                "client",
                KeyStoreFixtures.CLIENT.key,
                KeyStoreFixtures.CLIENT.certificate);
    }

    private static ClientIdentity load(Path file, String type, String alias, String password)
            throws Exception {
        return load(file, type, alias, password, "store-secret");
    }

    private static ClientIdentity load(
            Path file, String type, String alias, String password, String storePassword)
            throws Exception {
        char[] supplied = password.toCharArray();
        try {
            return ClientIdentity.load(
                    new KeyStoreSettings(type, file, storePassword), alias, supplied, null);
        } finally {
            assertArrayEquals(new char[supplied.length], supplied);
        }
    }

    static void assertIdentity(ClientIdentity actual, KeyStoreFixtures.Identity expected) {
        assertEquals(expected.certificate, actual.chain()[0]);
        assertArrayEquals(
                expected.key.getPrivate().getEncoded(), actual.keyPair().getPrivate().getEncoded());
        assertArrayEquals(
                expected.key.getPublic().getEncoded(), actual.keyPair().getPublic().getEncoded());
        assertEquals(
                expected == KeyStoreFixtures.CLIENT ? "urn:test:client" : "urn:test:other",
                actual.applicationUri());
    }

    static void assertUnencryptedSafeContents(Path file, boolean mac, boolean shroudedKey)
            throws Exception {
        Pfx pfx = Pfx.getInstance(ASN1Primitive.fromByteArray(Files.readAllBytes(file)));
        assertEquals(mac, pfx.getMacData() != null);
        assertEquals(PKCSObjectIdentifiers.data, pfx.getAuthSafe().getContentType());
        AuthenticatedSafe safe =
                AuthenticatedSafe.getInstance(
                        ASN1Primitive.fromByteArray(
                                ASN1OctetString.getInstance(pfx.getAuthSafe().getContent())
                                        .getOctets()));
        int certificates = 0, keys = 0;
        for (ContentInfo content : safe.getContentInfo()) {
            assertEquals(PKCSObjectIdentifiers.data, content.getContentType());
            ASN1Sequence bags =
                    ASN1Sequence.getInstance(
                            ASN1Primitive.fromByteArray(
                                    ASN1OctetString.getInstance(content.getContent()).getOctets()));
            for (ASN1Encodable encoded : bags) {
                SafeBag bag = SafeBag.getInstance(encoded);
                if (PKCSObjectIdentifiers.certBag.equals(bag.getBagId())) certificates++;
                else {
                    assertEquals(PKCSObjectIdentifiers.pkcs8ShroudedKeyBag, bag.getBagId());
                    EncryptedPrivateKeyInfo encrypted =
                            EncryptedPrivateKeyInfo.getInstance(bag.getBagValue());
                    assertEquals(
                            PKCSObjectIdentifiers.pbeWithSHAAnd3_KeyTripleDES_CBC,
                            encrypted.getEncryptionAlgorithm().getAlgorithm());
                    assertTrue(encrypted.getEncryptedData().length > 0);
                    keys++;
                }
            }
        }
        assertEquals(1, certificates);
        assertEquals(shroudedKey ? 1 : 0, keys);
    }
}
