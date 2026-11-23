/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Looks for multiple PEM certificate headers in the directories read by the selected trust source.
 */
final class PemBundleCheck {
    private static final String HEADER = "-----BEGIN CERTIFICATE-----";
    private final byte[] buffer = new byte[64 * 1024];
    private int remainingBytes = 512 * 1024;
    private int remainingEntries = 128;
    private boolean foundBundle;
    private boolean incomplete;

    static String inspect(Path pki, boolean directoryTrust, String issuerDirectory) {
        PemBundleCheck check = new PemBundleCheck();
        if (directoryTrust) check.directory(pki.resolve("trusted/certs"));
        check.directory(pki.resolve(issuerDirectory));
        String result =
                check.foundBundle
                        ? " A PKI file contains multiple PEM certificate headers. Milo loads only"
                                + " the first certificate from each directory file. Use one"
                                + " certificate per file. Put only approved trust anchors in the"
                                + " selected trust source and any missing intermediates in "
                                + issuerDirectory
                                + "."
                        : "";
        if (check.incomplete)
            result += " The PEM bundle check may have covered only part of the directory contents.";
        return result;
    }

    private void directory(Path directory) {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
            for (Path file : files) {
                if (remainingEntries == 0 || remainingBytes == 0) {
                    incomplete = true;
                    return;
                }
                remainingEntries--;
                try {
                    inspectFile(file);
                } catch (IOException | RuntimeException unreadable) {
                    incomplete = true;
                }
            }
        } catch (IOException | RuntimeException unreadable) {
            incomplete = true;
        }
    }

    private void inspectFile(Path file) throws IOException {
        BasicFileAttributes attributes =
                Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            incomplete = true;
            return;
        }
        ByteBuffer bytes = ByteBuffer.wrap(buffer, 0, Math.min(remainingBytes, buffer.length));
        try (SeekableByteChannel input = Files.newByteChannel(file, LinkOption.NOFOLLOW_LINKS)) {
            while (bytes.hasRemaining()) {
                int read = input.read(bytes);
                if (read < 0) break;
                remainingBytes -= read;
            }
            if (!bytes.hasRemaining() && input.size() > bytes.position()) incomplete = true;
        }
        String prefix = new String(buffer, 0, bytes.position(), StandardCharsets.US_ASCII);
        int first = prefix.indexOf(HEADER);
        if (first >= 0 && prefix.indexOf(HEADER, first + HEADER.length()) >= 0) foundBundle = true;
    }
}
