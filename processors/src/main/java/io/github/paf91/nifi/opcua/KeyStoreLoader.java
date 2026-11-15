/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

final class KeyStoreLoader {
    private KeyStoreLoader() {}

    static KeyStore load(Path path, String type, char[] password) throws Exception {
        char[] supplied = password == null ? new char[0] : password;
        try {
            if (path == null) throw new IllegalArgumentException("Keystore file is required");
            if (!"JKS".equals(type) && !"PKCS12".equals(type))
                throw new IllegalArgumentException("Keystore type must be JKS or PKCS12");
            try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(path))) {
                in.mark(4);
                int first = in.read();
                boolean matches =
                        "JKS".equals(type)
                                ? first == 0xfe
                                        && in.read() == 0xed
                                        && in.read() == 0xfe
                                        && in.read() == 0xed
                                : first == 0x30;
                in.reset();
                if (!matches)
                    throw new IOException(
                            "Keystore file does not match the selected " + type + " format");
                KeyStore store = KeyStore.getInstance(type);
                // An empty password still checks a protected store's integrity. Null would not.
                store.load(in, supplied);
                return store;
            }
        } finally {
            Arrays.fill(supplied, '\0');
        }
    }

    static List<X509Certificate> trustedCertificates(KeyStoreSettings settings) throws Exception {
        KeyStore store = load(settings.path(), settings.type(), settings.password().toCharArray());
        if (store.size() == 0)
            throw new IllegalArgumentException(
                    "Server truststore exposes no entries after loading. It may be empty"
                            + ("PKCS12".equals(settings.type())
                                    ? " or contain certificates without Java trusted-certificate"
                                            + " markers that this provider does not expose"
                                    : "")
                            + ". Import trusted certificates with keytool.");
        List<X509Certificate> certificates = new ArrayList<>();
        Enumeration<String> aliases = store.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (!store.isCertificateEntry(alias)) continue;
            Certificate certificate = store.getCertificate(alias);
            if (!(certificate instanceof X509Certificate))
                throw new IllegalArgumentException(
                        "Server truststore entries must be X.509 certificates");
            certificates.add((X509Certificate) certificate);
        }
        if (certificates.isEmpty())
            throw new IllegalArgumentException(
                    "Server truststore must contain at least one trusted certificate entry;"
                            + " private-key certificate chains are not trust anchors");
        return Collections.unmodifiableList(certificates);
    }
}
