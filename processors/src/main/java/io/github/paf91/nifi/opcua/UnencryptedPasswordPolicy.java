/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

/** Controls when a password may be sent without token or channel encryption. */
enum UnencryptedPasswordPolicy {
    NONE_CHANNELS_ONLY("None channels only"),
    NEVER("Never"),
    ALWAYS("Always");

    private final String value;

    UnencryptedPasswordPolicy(String value) {
        this.value = value;
    }

    String value() {
        return value;
    }

    static UnencryptedPasswordPolicy parse(String value) {
        for (UnencryptedPasswordPolicy policy : values())
            if (policy.value.equals(value)) return policy;
        throw new IllegalArgumentException("Unknown Allow Unencrypted Password: " + value);
    }
}
