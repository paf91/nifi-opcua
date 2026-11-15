/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.eclipse.milo.opcua.stack.core.security.FileBasedTrustListManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.io.IOException;
import java.util.stream.Stream;

class TrustDirectoryInitializationTest {
    @Test
    void successfulInitializationReturnsTheManagerWithoutClosingIt() throws Exception {
        FileBasedTrustListManager manager = mock(FileBasedTrustListManager.class);

        assertSame(manager, TrustDirectory.initialize(manager));

        verify(manager).initialize();
        verify(manager, never()).close();
        verifyNoMoreInteractions(manager);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedInitializationClosesExactlyOnceAndPreservesTheOriginalFailure(boolean error)
            throws Exception {
        FileBasedTrustListManager manager = mock(FileBasedTrustListManager.class);
        Throwable original = failure(error, "initialize");
        doThrow(original).when(manager).initialize();

        Throwable actual = assertThrows(Throwable.class, () -> TrustDirectory.initialize(manager));

        assertSame(original, actual);
        assertEquals(0, actual.getSuppressed().length);
        verify(manager).initialize();
        verify(manager).close();
        verifyNoMoreInteractions(manager);
    }

    static Stream<Object[]> cleanupFailures() {
        return Stream.of(false, true)
                .flatMap(
                        initializationError ->
                                Stream.of(false, true)
                                        .map(
                                                cleanupError ->
                                                        new Object[] {
                                                            initializationError, cleanupError
                                                        }));
    }

    @ParameterizedTest(name = "initialization error={0}, cleanup error={1}")
    @MethodSource("cleanupFailures")
    void cleanupFailuresAreSuppressedWithoutReplacingTheInitializationFailure(
            boolean initializationError, boolean cleanupError) throws Exception {
        FileBasedTrustListManager manager = mock(FileBasedTrustListManager.class);
        Throwable original = failure(initializationError, "initialize");
        Throwable cleanup = failure(cleanupError, "close");
        doThrow(original).when(manager).initialize();
        doThrow(cleanup).when(manager).close();

        Throwable actual = assertThrows(Throwable.class, () -> TrustDirectory.initialize(manager));

        assertSame(original, actual);
        assertArrayEquals(new Throwable[] {cleanup}, actual.getSuppressed());
        verify(manager).initialize();
        verify(manager).close();
        verifyNoMoreInteractions(manager);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cleanupDoesNotSuppressTheOriginalFailureOntoItself(boolean error) throws Exception {
        FileBasedTrustListManager manager = mock(FileBasedTrustListManager.class);
        Throwable original = failure(error, "same failure");
        doThrow(original).when(manager).initialize();
        doThrow(original).when(manager).close();

        Throwable actual = assertThrows(Throwable.class, () -> TrustDirectory.initialize(manager));

        assertSame(original, actual);
        assertEquals(0, actual.getSuppressed().length);
        verify(manager).initialize();
        verify(manager).close();
        verifyNoMoreInteractions(manager);
    }

    private static Throwable failure(boolean error, String message) {
        return error ? new AssertionError(message) : new IOException(message);
    }
}
