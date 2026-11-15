/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.stream.Stream;

class ClientIdentityFormatTest {
    @TempDir Path dir;
    static KeyPair key;
    static X509Certificate certificate;

    @BeforeAll
    static void createIdentity() throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        key = generator.generateKeyPair();
        certificate =
                new SelfSignedCertificateBuilder(key)
                        .setCommonName("Test")
                        .setCountryCode("US")
                        .setApplicationUri("urn:test:client")
                        .build();
    }

    @ParameterizedTest
    @ValueSource(strings = {"pem", "openssl-pem", "der"})
    void readsPrivateKeyIncludingOpenSslExportPreamble(String format) throws Exception {
        Path certFile = dir.resolve("client.der");
        Path keyFile = dir.resolve("client.key");
        Files.write(certFile, certificate.getEncoded());
        if ("der".equals(format)) Files.write(keyFile, key.getPrivate().getEncoded());
        else {
            StringWriter text = new StringWriter();
            if ("openssl-pem".equals(format))
                text.write(
                        "Bag Attributes\n"
                                + "    friendlyName: client\n"
                                + "    localKeyID: 01 02 03 04\n"
                                + "Key Attributes: <No Attributes>\n");
            try (JcaPEMWriter writer = new JcaPEMWriter(text)) {
                writer.writeObject(key.getPrivate());
            }
            Files.write(keyFile, text.toString().getBytes(StandardCharsets.US_ASCII));
        }
        char[] password = "unused-for-unencrypted-key".toCharArray();
        ClientIdentity loaded = ClientIdentity.load(certFile, keyFile, password, null);
        assertEquals("urn:test:client", loaded.applicationUri());
        assertEquals(certificate, loaded.chain()[0]);
        assertArrayEquals(
                key.getPrivate().getEncoded(), loaded.keyPair().getPrivate().getEncoded());
        assertArrayEquals(new char[password.length], password);
    }

    @Test
    void clearsPasswordWhenCertificateFileIsMissing() throws Exception {
        Path certFile = dir.resolve("missing.der");
        Path keyFile = dir.resolve("client.key");
        Files.write(keyFile, key.getPrivate().getEncoded());
        char[] password = {'t', 'e', 's', 't'};

        NoSuchFileException failure =
                assertThrows(
                        NoSuchFileException.class,
                        () -> ClientIdentity.load(certFile, keyFile, password, null));

        assertEquals(certFile.toString(), failure.getFile());
        assertArrayEquals(new char[password.length], password);
    }

    @Test
    void clearsPasswordWhenCertificateIsInvalid() throws Exception {
        Path certFile = dir.resolve("invalid.der");
        Path keyFile = dir.resolve("client.key");
        Files.write(certFile, new byte[] {0x30, 0x01, 0x00});
        Files.write(keyFile, key.getPrivate().getEncoded());
        char[] password = {'t', 'e', 's', 't'};

        CertificateException failure =
                assertThrows(
                        CertificateException.class,
                        () -> ClientIdentity.load(certFile, keyFile, password, null));

        assertNotNull(failure.getMessage());
        assertFalse(failure.getMessage().trim().isEmpty());
        assertArrayEquals(new char[password.length], password);
    }

    @Test
    void clearsPasswordWhenCertificateFileIsEmpty() throws Exception {
        Path certFile = Files.createFile(dir.resolve("empty.der"));
        Path keyFile = dir.resolve("client.key");
        Files.write(keyFile, key.getPrivate().getEncoded());
        char[] password = {'t', 'e', 's', 't'};

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ClientIdentity.load(certFile, keyFile, password, null));

        assertEquals("Certificate file is empty", failure.getMessage());
        assertArrayEquals(new char[password.length], password);
    }

    @Test
    void clearsPasswordWhenApplicationUriOverrideDoesNotMatch() throws Exception {
        Path certFile = dir.resolve("client.der");
        Path keyFile = dir.resolve("client.key");
        Files.write(certFile, certificate.getEncoded());
        Files.write(keyFile, key.getPrivate().getEncoded());
        char[] password = {'t', 'e', 's', 't'};

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ClientIdentity.load(certFile, keyFile, password, "urn:wrong:client"));

        assertEquals(
                "Application URI override must match the client certificate URI SAN",
                failure.getMessage());
        assertArrayEquals(new char[password.length], password);
    }

    @Test
    void clearsPasswordWhenPrivateKeyFileIsMissing() throws Exception {
        Path certFile = dir.resolve("client.der");
        Path keyFile = dir.resolve("missing.key");
        Files.write(certFile, certificate.getEncoded());
        char[] password = {'t', 'e', 's', 't'};

        NoSuchFileException failure =
                assertThrows(
                        NoSuchFileException.class,
                        () -> ClientIdentity.load(certFile, keyFile, password, null));

        assertEquals(keyFile.toString(), failure.getFile());
        assertArrayEquals(new char[password.length], password);
    }

    @Test
    void missingCertificateWithNullPasswordStillReportsMissingFile() {
        Path certFile = dir.resolve("missing.der");
        Path keyFile = dir.resolve("client.key");

        NoSuchFileException failure =
                assertThrows(
                        NoSuchFileException.class,
                        () -> ClientIdentity.load(certFile, keyFile, null, null));

        assertEquals(certFile.toString(), failure.getFile());
    }

    static Stream<Object[]> encryptedKeyFormats() {
        return Stream.of("traditional", "pkcs8")
                .flatMap(
                        format ->
                                Stream.of("key-secret", "wrong", "", null)
                                        .map(password -> new Object[] {format, password}));
    }

    @ParameterizedTest(name = "encrypted {0}, password={1}")
    @MethodSource("encryptedKeyFormats")
    void encryptedPemStillRequiresTheIndependentKeyPassword(String format, String suppliedPassword)
            throws Exception {
        Path certFile = dir.resolve("client.der");
        Path keyFile = dir.resolve("encrypted.pem");
        Files.write(certFile, certificate.getEncoded());
        try (JcaPEMWriter writer = new JcaPEMWriter(Files.newBufferedWriter(keyFile))) {
            if ("traditional".equals(format))
                writer.writeObject(
                        key.getPrivate(),
                        new JcePEMEncryptorBuilder("AES-128-CBC")
                                .setProvider(new BouncyCastleProvider())
                                .build("key-secret".toCharArray()));
            else
                writer.writeObject(
                        new JcaPKCS8Generator(
                                key.getPrivate(),
                                new JceOpenSSLPKCS8EncryptorBuilder(
                                                PKCSObjectIdentifiers
                                                        .pbeWithSHAAnd3_KeyTripleDES_CBC)
                                        .setProvider(new BouncyCastleProvider())
                                        .setPassword("key-secret".toCharArray())
                                        .build()));
        }
        String encoded = new String(Files.readAllBytes(keyFile), StandardCharsets.US_ASCII);
        assertTrue(
                encoded.contains(
                        "traditional".equals(format)
                                ? "Proc-Type: 4,ENCRYPTED"
                                : "-----BEGIN ENCRYPTED PRIVATE KEY-----"));
        char[] password = suppliedPassword == null ? null : suppliedPassword.toCharArray();
        try {
            if ("key-secret".equals(suppliedPassword)) {
                ClientIdentity loaded = ClientIdentity.load(certFile, keyFile, password, null);
                assertEquals("urn:test:client", loaded.applicationUri());
                assertEquals(certificate, loaded.chain()[0]);
                assertArrayEquals(
                        key.getPrivate().getEncoded(), loaded.keyPair().getPrivate().getEncoded());
            } else
                assertThrows(
                        Exception.class,
                        () -> ClientIdentity.load(certFile, keyFile, password, null));
        } finally {
            if (password != null) assertArrayEquals(new char[password.length], password);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"key", "uri"})
    void disablingServerValidationStillValidatesClientIdentity(String mismatch) throws Exception {
        Path certFile = dir.resolve("client.der"), keyFile = dir.resolve("client.key");
        Files.write(certFile, certificate.getEncoded());
        PrivateKey privateKey = key.getPrivate();
        if ("key".equals(mismatch)) {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            privateKey = generator.generateKeyPair().getPrivate();
        }
        Files.write(keyFile, privateKey.getEncoded());
        OpcSettings settings =
                new OpcSettings(
                        "opc.tcp://localhost:4840",
                        "uri".equals(mismatch) ? "urn:wrong:client" : null,
                        certFile,
                        keyFile,
                        "",
                        null,
                        "user",
                        "test-password",
                        100,
                        1000,
                        java.util.Collections.emptyList(),
                        OpcSecurity.DEFAULT,
                        ServerCertificateValidation.NONE);
        assertThrows(IllegalArgumentException.class, () -> ClientIdentity.load(settings));
    }
}
