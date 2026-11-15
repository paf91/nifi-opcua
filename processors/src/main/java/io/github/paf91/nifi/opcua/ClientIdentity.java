/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.*;
import org.bouncycastle.openssl.jcajce.*;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.*;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;

final class ClientIdentity {
    private final KeyPair keyPair;
    private final X509Certificate[] chain;
    private final String applicationUri;

    ClientIdentity(KeyPair keyPair, X509Certificate[] chain, String applicationUri) {
        this.keyPair = keyPair;
        this.chain = chain;
        this.applicationUri = applicationUri;
    }

    KeyPair keyPair() {
        return keyPair;
    }

    X509Certificate[] chain() {
        return chain;
    }

    String applicationUri() {
        return applicationUri;
    }

    static ClientIdentity load(OpcSettings settings) throws Exception {
        if (!settings.security().requiresClientIdentity()) return null;
        if (settings.clientKeyStore() != null)
            return load(
                    settings.clientKeyStore(),
                    settings.clientKeyAlias(),
                    settings.privateKeyPassword().toCharArray(),
                    settings.applicationUri());
        return load(
                settings.certificate(),
                settings.privateKey(),
                settings.privateKeyPassword().toCharArray(),
                settings.applicationUri());
    }

    static ClientIdentity load(
            KeyStoreSettings settings, String alias, char[] keyPassword, String applicationUri)
            throws Exception {
        char[] supplied = keyPassword == null ? new char[0] : keyPassword;
        try {
            KeyStore store =
                    KeyStoreLoader.load(
                            settings.path(), settings.type(), settings.password().toCharArray());
            String selected = alias == null || alias.isEmpty() ? null : alias;
            if (selected == null) {
                java.util.Enumeration<String> aliases = store.aliases();
                while (aliases.hasMoreElements()) {
                    String candidate = aliases.nextElement();
                    if (!store.entryInstanceOf(candidate, KeyStore.PrivateKeyEntry.class)) continue;
                    if (selected != null)
                        throw new IllegalArgumentException(
                                "Client keystore contains multiple private-key entries; set Client"
                                        + " Key Alias");
                    selected = candidate;
                }
                if (selected == null)
                    throw new IllegalArgumentException(
                            "Client keystore has no supported private-key entry; use a JKS or"
                                + " PKCS12 private-key entry with a certificate chain, or PEM/DER"
                                + " files");
            } else if (!store.containsAlias(selected)) {
                throw new IllegalArgumentException(
                        "Client Key Alias was not found in the client keystore");
            }
            if (!store.entryInstanceOf(selected, KeyStore.PrivateKeyEntry.class))
                throw new IllegalArgumentException(
                        "Client Key Alias must identify a private-key entry");
            java.security.cert.Certificate[] certificates = store.getCertificateChain(selected);
            if (certificates == null || certificates.length == 0)
                throw new IllegalArgumentException(
                        "Client keystore private-key entry has no certificate chain");
            X509Certificate[] chain = new X509Certificate[certificates.length];
            for (int i = 0; i < certificates.length; i++) {
                if (!(certificates[i] instanceof X509Certificate))
                    throw new IllegalArgumentException(
                            "Client certificate chain must contain X.509 certificates");
                chain[i] = (X509Certificate) certificates[i];
            }
            String resolvedUri = validateCertificate(chain, applicationUri);
            Key key = store.getKey(selected, supplied);
            if (!(key instanceof PrivateKey))
                throw new IllegalArgumentException(
                        "Client Key Alias must identify a private-key entry");
            return identity((PrivateKey) key, chain, resolvedUri);
        } finally {
            Arrays.fill(supplied, '\0');
        }
    }

    static String applicationUri(OpcSettings settings, ClientIdentity identity) {
        if (identity != null) return identity.applicationUri();
        return settings.applicationUri() == null
                ? OpcSecurity.DEFAULT_APPLICATION_URI
                : settings.applicationUri();
    }

    static String resolveApplicationUri(X509Certificate certificate, String override)
            throws CertificateParsingException {
        java.util.List<String> uris = new java.util.ArrayList<>();
        if (certificate.getSubjectAlternativeNames() != null) {
            for (java.util.List<?> name : certificate.getSubjectAlternativeNames()) {
                if (Integer.valueOf(6).equals(name.get(0)) && name.get(1) instanceof String)
                    uris.add((String) name.get(1));
            }
        }
        if (uris.size() != 1 || uris.get(0).trim().isEmpty())
            throw new IllegalArgumentException(
                    "Client certificate must contain exactly one application URI in its subject"
                            + " alternative names");
        String uri = uris.get(0);
        try {
            if (!java.net.URI.create(uri).isAbsolute()) throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Client certificate application URI must be a valid absolute URI");
        }
        if (override != null && !override.trim().isEmpty() && !override.equals(uri))
            throw new IllegalArgumentException(
                    "Application URI override must match the client certificate URI SAN");
        return uri;
    }

    static ClientIdentity load(
            Path certificate, Path keyFile, char[] password, String applicationUri)
            throws Exception {
        try {
            return loadIdentity(certificate, keyFile, password, applicationUri);
        } finally {
            if (password != null) Arrays.fill(password, '\0');
        }
    }

    private static ClientIdentity loadIdentity(
            Path certificate, Path keyFile, char[] password, String applicationUri)
            throws Exception {
        X509Certificate[] chain;
        try (InputStream in = Files.newInputStream(certificate)) {
            chain =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(in)
                            .toArray(new X509Certificate[0]);
        }
        String resolvedUri = validateCertificate(chain, applicationUri);
        byte[] bytes = Files.readAllBytes(keyFile);
        PrivateKey privateKey;
        try {
            // OpenSSL PKCS#12 exports can put Bag Attributes before the PEM boundary.
            if (new String(bytes, StandardCharsets.US_ASCII).contains("-----BEGIN ")) {
                BouncyCastleProvider provider = new BouncyCastleProvider();
                JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(provider);
                try (java.io.StringReader reader =
                                new java.io.StringReader(
                                        new String(bytes, StandardCharsets.US_ASCII));
                        PEMParser parser = new PEMParser(reader)) {
                    Object value = parser.readObject();
                    if (value instanceof PEMEncryptedKeyPair)
                        value =
                                ((PEMEncryptedKeyPair) value)
                                        .decryptKeyPair(
                                                new JcePEMDecryptorProviderBuilder()
                                                        .setProvider(provider)
                                                        .build(password));
                    if (value instanceof PKCS8EncryptedPrivateKeyInfo)
                        value =
                                ((PKCS8EncryptedPrivateKeyInfo) value)
                                        .decryptPrivateKeyInfo(
                                                new JceOpenSSLPKCS8DecryptorProviderBuilder()
                                                        .setProvider(provider)
                                                        .build(password));
                    if (value instanceof PEMKeyPair)
                        privateKey = converter.getKeyPair((PEMKeyPair) value).getPrivate();
                    else if (value instanceof PrivateKeyInfo)
                        privateKey = converter.getPrivateKey((PrivateKeyInfo) value);
                    else throw new IllegalArgumentException("Unsupported PEM private key format");
                }
            } else {
                privateKey =
                        KeyFactory.getInstance("RSA")
                                .generatePrivate(new PKCS8EncodedKeySpec(bytes));
            }
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
        return identity(privateKey, chain, resolvedUri);
    }

    private static String validateCertificate(X509Certificate[] chain, String applicationUri)
            throws CertificateException {
        if (chain.length == 0) throw new IllegalArgumentException("Certificate file is empty");
        X509Certificate leaf = chain[0];
        leaf.checkValidity();
        if (!(leaf.getPublicKey() instanceof RSAPublicKey)
                || ((RSAPublicKey) leaf.getPublicKey()).getModulus().bitLength() < 2048)
            throw new IllegalArgumentException(
                    "Secured connections require an RSA certificate with at least 2048 bits");
        return resolveApplicationUri(leaf, applicationUri);
    }

    private static ClientIdentity identity(
            PrivateKey privateKey, X509Certificate[] chain, String resolvedUri) throws Exception {
        X509Certificate leaf = chain[0];
        Signature signature = Signature.getInstance("SHA256withRSA");
        byte[] challenge = new byte[32];
        new SecureRandom().nextBytes(challenge);
        signature.initSign(privateKey);
        signature.update(challenge);
        byte[] signed = signature.sign();
        signature.initVerify(leaf);
        signature.update(challenge);
        if (!signature.verify(signed))
            throw new IllegalArgumentException("Private key does not match certificate");
        return new ClientIdentity(new KeyPair(leaf.getPublicKey(), privateKey), chain, resolvedUri);
    }
}
