/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

class TrustDirectoryTest {
    @TempDir Path dir;
    private static final byte[] MARKER = new byte[] {11, 22, 33};

    static Stream<String> directoryPaths() {
        return TestServerTrust.directoryPaths().stream();
    }

    @ParameterizedTest(name = "regular file at {0}")
    @MethodSource("directoryPaths")
    void preparationRejectsARegularFileAtEveryRequiredDirectory(String relative) throws Exception {
        Path pki = dir.resolve("pki");
        Path file = pki.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.write(file, MARKER);

        assertThrows(IOException.class, () -> TrustDirectory.prepare(pki));

        assertTrue(Files.isRegularFile(file));
        assertArrayEquals(MARKER, Files.readAllBytes(file));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void opensAndClosesNewOrExistingDirectoriesWithoutChangingSentinels(boolean existing)
            throws Exception {
        Path pki = dir.resolve("pki");
        if (existing) Files.createDirectories(pki);
        Path sentinel = existing ? pki.resolve("keep.marker") : dir.resolve("keep.marker");
        Files.write(sentinel, MARKER);

        try (AutoCloseable trust = TrustDirectory.open(pki)) {
            assertNotNull(trust);
            for (String relative : TestServerTrust.directoryPaths())
                assertTrue(Files.isDirectory(pki.resolve(relative)), relative);
        }

        assertArrayEquals(MARKER, Files.readAllBytes(sentinel));
    }

    @Test
    void acceptsAnExistingDirectorySymlinkAsThePkiRoot() throws Exception {
        Path target = Files.createDirectory(dir.resolve("real-pki"));
        Path pki = createSymbolicLink(dir.resolve("linked-pki"), target);
        Path sentinel = target.resolve("keep.marker");
        Files.write(sentinel, MARKER);

        try (AutoCloseable trust = TrustDirectory.open(pki)) {
            assertNotNull(trust);
            for (String relative : TestServerTrust.directoryPaths())
                assertTrue(Files.isDirectory(target.resolve(relative)), relative);
        }

        assertTrue(Files.isSymbolicLink(pki));
        assertArrayEquals(MARKER, Files.readAllBytes(sentinel));
    }

    @Test
    void acceptsAnExistingDirectorySymlinkAtAWatchedChild() throws Exception {
        Path target = Files.createDirectory(dir.resolve("external-certificates"));
        Path pki = dir.resolve("pki");
        Files.createDirectories(pki.resolve("trusted"));
        Path linked = createSymbolicLink(pki.resolve("trusted/certs"), target);

        try (AutoCloseable trust = TrustDirectory.open(pki)) {
            assertNotNull(trust);
            assertTrue(Files.isDirectory(linked));
        }

        assertTrue(Files.isSymbolicLink(linked));
        assertTrue(Files.isDirectory(target));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "trusted/certs"})
    void preparationRejectsSymlinksToRegularFiles(String relative) throws Exception {
        Path target = dir.resolve("regular-file");
        Files.write(target, MARKER);
        Path pki = dir.resolve("pki");
        Path linked = pki.resolve(relative);
        Files.createDirectories(linked.getParent());
        createSymbolicLink(linked, target);

        assertThrows(IOException.class, () -> TrustDirectory.prepare(pki));

        assertTrue(Files.isSymbolicLink(linked));
        assertArrayEquals(MARKER, Files.readAllBytes(target));
    }

    private static Path createSymbolicLink(Path link, Path target) throws IOException {
        try {
            return Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | SecurityException failure) {
            return Assumptions.abort("Symbolic links unavailable: " + failure);
        } catch (FileSystemException failure) {
            if ("\\".equals(link.getFileSystem().getSeparator())
                    && "A required privilege is not held by the client"
                            .equalsIgnoreCase(failure.getReason()))
                return Assumptions.abort("Symbolic links unavailable: " + failure);
            throw failure;
        }
    }
}
