/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import java.nio.file.Path;
import java.util.List;

final class OpcSettings {
    private final String endpoint;
    private final String applicationUri;
    private final Path certificate;
    private final Path privateKey;
    private final String privateKeyPassword;
    private final Path pki;
    private final String username;
    private final String password;
    private final int publishingInterval;
    private final int requestTimeout;
    private final List<TagDefinition> tags;
    private final OpcSecurity security;
    private final ServerCertificateValidation serverCertificateValidation;
    private final OutputSettings output;
    private final KeyStoreSettings clientKeyStore;
    private final String clientKeyAlias;
    private final KeyStoreSettings serverTrustStore;

    OpcSettings(
            String endpoint,
            String applicationUri,
            Path certificate,
            Path privateKey,
            String privateKeyPassword,
            Path pki,
            String username,
            String password,
            int publishingInterval,
            int requestTimeout,
            List<TagDefinition> tags) {
        this(
                endpoint,
                applicationUri,
                certificate,
                privateKey,
                privateKeyPassword,
                pki,
                username,
                password,
                publishingInterval,
                requestTimeout,
                tags,
                OpcSecurity.DEFAULT);
    }

    OpcSettings(
            String endpoint,
            String applicationUri,
            Path certificate,
            Path privateKey,
            String privateKeyPassword,
            Path pki,
            String username,
            String password,
            int publishingInterval,
            int requestTimeout,
            List<TagDefinition> tags,
            OpcSecurity security) {
        this(
                endpoint,
                applicationUri,
                certificate,
                privateKey,
                privateKeyPassword,
                pki,
                username,
                password,
                publishingInterval,
                requestTimeout,
                tags,
                security,
                ServerCertificateValidation.FULL);
    }

    OpcSettings(
            String endpoint,
            String applicationUri,
            Path certificate,
            Path privateKey,
            String privateKeyPassword,
            Path pki,
            String username,
            String password,
            int publishingInterval,
            int requestTimeout,
            List<TagDefinition> tags,
            OpcSecurity security,
            ServerCertificateValidation serverCertificateValidation) {
        this(
                endpoint,
                applicationUri,
                certificate,
                privateKey,
                privateKeyPassword,
                pki,
                username,
                password,
                publishingInterval,
                requestTimeout,
                tags,
                security,
                serverCertificateValidation,
                OutputSettings.DEFAULT,
                null,
                null,
                null);
    }

    private OpcSettings(
            String endpoint,
            String applicationUri,
            Path certificate,
            Path privateKey,
            String privateKeyPassword,
            Path pki,
            String username,
            String password,
            int publishingInterval,
            int requestTimeout,
            List<TagDefinition> tags,
            OpcSecurity security,
            ServerCertificateValidation serverCertificateValidation,
            OutputSettings output,
            KeyStoreSettings clientKeyStore,
            String clientKeyAlias,
            KeyStoreSettings serverTrustStore) {
        this.endpoint = endpoint;
        this.applicationUri = applicationUri;
        this.certificate = certificate;
        this.privateKey = privateKey;
        this.privateKeyPassword = privateKeyPassword;
        this.pki = pki;
        this.username = username;
        this.password = password;
        this.publishingInterval = publishingInterval;
        this.requestTimeout = requestTimeout;
        this.tags = tags;
        this.security = security;
        this.serverCertificateValidation =
                java.util.Objects.requireNonNull(serverCertificateValidation);
        this.output = output;
        this.clientKeyStore = clientKeyStore;
        this.clientKeyAlias = clientKeyAlias;
        this.serverTrustStore = serverTrustStore;
    }

    OpcSettings withOutput(OutputSettings output) {
        return new OpcSettings(
                endpoint,
                applicationUri,
                certificate,
                privateKey,
                privateKeyPassword,
                pki,
                username,
                password,
                publishingInterval,
                requestTimeout,
                tags,
                security,
                serverCertificateValidation,
                java.util.Objects.requireNonNull(output),
                clientKeyStore,
                clientKeyAlias,
                serverTrustStore);
    }

    OpcSettings withKeyStores(
            KeyStoreSettings clientKeyStore,
            String clientKeyAlias,
            KeyStoreSettings serverTrustStore) {
        return new OpcSettings(
                endpoint,
                applicationUri,
                certificate,
                privateKey,
                privateKeyPassword,
                pki,
                username,
                password,
                publishingInterval,
                requestTimeout,
                tags,
                security,
                serverCertificateValidation,
                output,
                clientKeyStore,
                clientKeyAlias,
                serverTrustStore);
    }

    KeyStoreSettings clientKeyStore() {
        return clientKeyStore;
    }

    String clientKeyAlias() {
        return clientKeyAlias;
    }

    KeyStoreSettings serverTrustStore() {
        return serverTrustStore;
    }

    OutputSettings output() {
        return output;
    }

    String endpoint() {
        return endpoint;
    }

    String applicationUri() {
        return applicationUri;
    }

    Path certificate() {
        return certificate;
    }

    Path privateKey() {
        return privateKey;
    }

    String privateKeyPassword() {
        return privateKeyPassword;
    }

    Path pki() {
        return pki;
    }

    String username() {
        return username;
    }

    String password() {
        return password;
    }

    int publishingInterval() {
        return publishingInterval;
    }

    int requestTimeout() {
        return requestTimeout;
    }

    List<TagDefinition> tags() {
        return tags;
    }

    OpcSecurity security() {
        return security;
    }

    ServerCertificateValidation serverCertificateValidation() {
        return serverCertificateValidation;
    }
}
