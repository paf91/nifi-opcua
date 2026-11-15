/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERBMPString;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.pkcs.PKCS12PfxPduBuilder;
import org.bouncycastle.pkcs.jcajce.*;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.X509Certificate;

final class KeyStoreFixtures {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    static final Identity CLIENT = identity("urn:test:client");
    static final Identity OTHER = identity("urn:test:other");
    static final ASN1ObjectIdentifier TRUSTED_KEY_USAGE =
            new ASN1ObjectIdentifier("2.16.840.1.113894.746875.1.1");

    static final class Identity {
        final KeyPair key;
        final X509Certificate certificate;

        Identity(KeyPair key, X509Certificate certificate) {
            this.key = key;
            this.certificate = certificate;
        }
    }

    private static Identity identity(String uri) {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair key = generator.generateKeyPair();
            return new Identity(
                    key,
                    new SelfSignedCertificateBuilder(key)
                            .setCommonName("Keystore test")
                            .setCountryCode("US")
                            .setApplicationUri(uri)
                            .build());
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static KeyStore empty(String type) throws Exception {
        KeyStore store = KeyStore.getInstance(type);
        store.load(null, new char[0]);
        return store;
    }

    static Path write(Path path, KeyStore store, String password) throws Exception {
        try (OutputStream out = Files.newOutputStream(path)) {
            store.store(out, password.toCharArray());
        }
        return path;
    }

    static Path identity(
            Path path,
            String type,
            String storePassword,
            String keyPassword,
            String alias,
            KeyPair key,
            X509Certificate... chain)
            throws Exception {
        KeyStore store = empty(type);
        store.setKeyEntry(alias, key.getPrivate(), keyPassword.toCharArray(), chain);
        return write(path, store, storePassword);
    }

    static Path trust(Path path, String type, String password, X509Certificate... certificates)
            throws Exception {
        KeyStore store = empty(type);
        for (int i = 0; i < certificates.length; i++)
            store.setCertificateEntry("trusted-" + i, certificates[i]);
        return write(path, store, password);
    }

    static Path pkcs12WithUnencryptedSafeContents(
            Path path,
            KeyPair key,
            X509Certificate certificate,
            String alias,
            String keyPassword,
            String macPassword)
            throws Exception {
        return pkcs12WithUnencryptedSafeContents(
                path, key, certificate, alias, keyPassword, macPassword, true);
    }

    static Path pkcs12WithUnencryptedSafeContents(
            Path path,
            KeyPair key,
            X509Certificate certificate,
            String alias,
            String keyPassword,
            String macPassword,
            boolean trustedMarker)
            throws Exception {
        PKCS12PfxPduBuilder pfx = new PKCS12PfxPduBuilder();
        byte[] localKeyId = new byte[] {1, 2, 3, 4};
        if (key != null) {
            JcaPKCS12SafeBagBuilder keyBag =
                    new JcaPKCS12SafeBagBuilder(
                            key.getPrivate(),
                            new JcePKCSPBEOutputEncryptorBuilder(
                                            PKCSObjectIdentifiers.pbeWithSHAAnd3_KeyTripleDES_CBC)
                                    .setProvider(new BouncyCastleProvider())
                                    .setIterationCount(2048)
                                    .build(keyPassword.toCharArray()));
            keyBag.addBagAttribute(
                    PKCSObjectIdentifiers.pkcs_9_at_friendlyName, new DERBMPString(alias));
            keyBag.addBagAttribute(
                    PKCSObjectIdentifiers.pkcs_9_at_localKeyId, new DEROctetString(localKeyId));
            pfx.addData(keyBag.build());
        }
        if (certificate != null) {
            JcaPKCS12SafeBagBuilder certBag = new JcaPKCS12SafeBagBuilder(certificate);
            certBag.addBagAttribute(
                    PKCSObjectIdentifiers.pkcs_9_at_friendlyName, new DERBMPString(alias));
            if (key == null && trustedMarker)
                certBag.addBagAttribute(TRUSTED_KEY_USAGE, KeyPurposeId.anyExtendedKeyUsage);
            else if (key != null)
                certBag.addBagAttribute(
                        PKCSObjectIdentifiers.pkcs_9_at_localKeyId, new DEROctetString(localKeyId));
            pfx.addData(certBag.build());
        }
        byte[] bytes =
                pfx.build(
                                macPassword == null
                                        ? null
                                        : new JcePKCS12MacCalculatorBuilder()
                                                .setProvider(new BouncyCastleProvider())
                                                .setIterationCount(2048),
                                macPassword == null ? null : macPassword.toCharArray())
                        .getEncoded();
        Files.write(path, bytes);
        return path;
    }

    private KeyStoreFixtures() {}
}
