/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import java.nio.file.Path;

final class KeyStoreSettings {
    private final String type;
    private final Path path;
    private final String password;

    KeyStoreSettings(String type, Path path, String password) {
        if (!"JKS".equals(type) && !"PKCS12".equals(type))
            throw new IllegalArgumentException("Keystore type must be JKS or PKCS12");
        this.type = type;
        this.path = path;
        this.password = password == null ? "" : password;
    }

    String type() {
        return type;
    }

    Path path() {
        return path;
    }

    String password() {
        return password;
    }
}
