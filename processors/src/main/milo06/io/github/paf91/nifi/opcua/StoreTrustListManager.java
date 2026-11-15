/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import com.google.common.collect.ImmutableList;

import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.TrustListManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class StoreTrustListManager implements TrustListManager, Closeable {
    private final ImmutableList<X509Certificate> trusted;
    private final DefaultTrustListManager directory;
    private final AtomicBoolean closed = new AtomicBoolean();

    StoreTrustListManager(KeyStoreSettings store, Path pki) throws Exception {
        trusted = ImmutableList.copyOf(KeyStoreLoader.trustedCertificates(store));
        directory = pki == null ? null : TrustDirectory.open(pki);
    }

    @Override
    public ImmutableList<X509Certificate> getTrustedCertificates() {
        return trusted;
    }

    @Override
    public ImmutableList<X509Certificate> getIssuerCertificates() {
        return directory == null ? ImmutableList.of() : directory.getIssuerCertificates();
    }

    @Override
    public ImmutableList<X509CRL> getTrustedCrls() {
        return directory == null ? ImmutableList.of() : directory.getTrustedCrls();
    }

    @Override
    public ImmutableList<X509CRL> getIssuerCrls() {
        return directory == null ? ImmutableList.of() : directory.getIssuerCrls();
    }

    @Override
    public ImmutableList<X509Certificate> getRejectedCertificates() {
        return directory == null ? ImmutableList.of() : directory.getRejectedCertificates();
    }

    @Override
    public void addRejectedCertificate(X509Certificate certificate) {
        if (directory != null) directory.addRejectedCertificate(certificate);
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

    @Override
    public boolean removeRejectedCertificate(ByteString thumbprint) {
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
