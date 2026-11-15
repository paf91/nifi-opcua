/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.eclipse.milo.opcua.stack.core.security.FileBasedTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.TrustListManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class StoreTrustListManager implements TrustListManager, Closeable {
    private final List<X509Certificate> trusted;
    private final FileBasedTrustListManager directory;
    private final DateTime loadedAt = DateTime.now();
    private final AtomicBoolean closed = new AtomicBoolean();

    StoreTrustListManager(KeyStoreSettings store, Path pki) throws Exception {
        trusted = KeyStoreLoader.trustedCertificates(store);
        directory = pki == null ? null : TrustDirectory.open(pki);
    }

    @Override
    public List<X509Certificate> getTrustedCertificates() {
        return trusted;
    }

    @Override
    public List<X509Certificate> getIssuerCertificates() {
        return directory == null ? Collections.emptyList() : directory.getIssuerCertificates();
    }

    @Override
    public List<X509CRL> getTrustedCrls() {
        return directory == null ? Collections.emptyList() : directory.getTrustedCrls();
    }

    @Override
    public List<X509CRL> getIssuerCrls() {
        return directory == null ? Collections.emptyList() : directory.getIssuerCrls();
    }

    @Override
    public DateTime getLastUpdateTime() {
        if (directory == null) return loadedAt;
        DateTime updated = directory.getLastUpdateTime();
        return updated.getUtcTime() > loadedAt.getUtcTime() ? updated : loadedAt;
    }

    @Override
    public void setIssuerCrls(List<X509CRL> crls) {
        throw readOnly();
    }

    @Override
    public void setTrustedCrls(List<X509CRL> crls) {
        throw readOnly();
    }

    @Override
    public void setIssuerCertificates(List<X509Certificate> certificates) {
        throw readOnly();
    }

    @Override
    public void setTrustedCertificates(List<X509Certificate> certificates) {
        throw readOnly();
    }

    @Override
    public void addIssuerCertificate(X509Certificate certificate) {
        throw readOnly();
    }

    @Override
    public void addTrustedCertificate(X509Certificate certificate) {
        throw readOnly();
    }

    @Override
    public boolean removeIssuerCertificate(ByteString thumbprint) {
        throw readOnly();
    }

    @Override
    public boolean removeTrustedCertificate(ByteString thumbprint) {
        throw readOnly();
    }

    private static UnsupportedOperationException readOnly() {
        return new UnsupportedOperationException("Configured server trust material is read-only");
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) && directory != null) directory.close();
    }
}
