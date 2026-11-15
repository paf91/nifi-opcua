/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.eclipse.milo.opcua.stack.core.security.FileBasedTrustListManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class TrustDirectory {
    private TrustDirectory() {}

    static void prepare(Path pki) throws IOException {
        for (String name :
                new String[] {
                    "",
                    "issuer",
                    "issuer/certs",
                    "issuer/crl",
                    "trusted",
                    "trusted/certs",
                    "trusted/crl"
                }) {
            Path directory = pki.resolve(name);
            if (!Files.isDirectory(directory)) Files.createDirectories(directory);
        }
    }

    static FileBasedTrustListManager open(Path pki) throws IOException {
        prepare(pki);
        return initialize(
                new FileBasedTrustListManager(
                        pki.resolve("issuer/certs"),
                        pki.resolve("issuer/crl"),
                        pki.resolve("trusted/certs"),
                        pki.resolve("trusted/crl")));
    }

    static FileBasedTrustListManager initialize(FileBasedTrustListManager manager)
            throws IOException {
        try {
            manager.initialize();
            return manager;
        } catch (Exception | Error failure) {
            try {
                manager.close();
            } catch (Exception | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }
}
