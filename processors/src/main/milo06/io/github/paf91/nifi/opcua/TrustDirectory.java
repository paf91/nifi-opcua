/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class TrustDirectory {
    private TrustDirectory() {}

    static void prepare(Path pki) throws IOException {
        for (String name :
                new String[] {
                    "", "issuers", "issuers/certs", "issuers/crl",
                    "trusted", "trusted/certs", "trusted/crl", "rejected"
                }) {
            Path directory = pki.resolve(name);
            if (!Files.isDirectory(directory)) Files.createDirectories(directory);
        }
    }

    static DefaultTrustListManager open(Path pki) throws IOException {
        // Reject ordinary path-is-file mistakes before the SDK allocates its watcher.
        prepare(pki);
        return new DefaultTrustListManager(pki.toFile());
    }
}
