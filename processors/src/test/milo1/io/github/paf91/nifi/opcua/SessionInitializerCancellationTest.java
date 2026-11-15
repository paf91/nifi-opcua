/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.UaRequestMessageType;
import org.eclipse.milo.opcua.stack.core.types.UaResponseMessageType;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.enumerated.ApplicationType;
import org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType;
import org.eclipse.milo.opcua.stack.core.types.structured.*;
import org.eclipse.milo.opcua.stack.core.util.Unit;
import org.eclipse.milo.opcua.stack.transport.client.OpcClientTransport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class SessionInitializerCancellationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void realSessionInitializerCompletesAndAllowsCleanupAfterCancellation(boolean cancel)
            throws Exception {
        ConnectionAttempt attempt = new ConnectionAttempt();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        OpcClientTransport transport = mock(OpcClientTransport.class, RETURNS_DEEP_STUBS);
        when(transport.getConfig().getExecutor()).thenReturn(executor);
        when(transport.connect(any())).thenReturn(CompletableFuture.completedFuture(Unit.VALUE));
        when(transport.disconnect()).thenReturn(CompletableFuture.completedFuture(Unit.VALUE));
        EndpointDescription bare =
                OpcSecurityTest.endpoint(
                        "None", "None", OpcSecurityTest.token(UserTokenType.Anonymous, null));
        EndpointDescription endpoint =
                new EndpointDescription(
                        bare.getEndpointUrl(),
                        new ApplicationDescription(
                                "urn:test:server",
                                "urn:test",
                                LocalizedText.english("Test"),
                                ApplicationType.Server,
                                null,
                                null,
                                new String[0]),
                        ByteString.NULL_VALUE,
                        bare.getSecurityMode(),
                        bare.getSecurityPolicyUri(),
                        bare.getUserIdentityTokens(),
                        bare.getTransportProfileUri(),
                        bare.getSecurityLevel());
        OpcSettings settings =
                new OpcSettings(
                        endpoint.getEndpointUrl(),
                        null,
                        null,
                        null,
                        "",
                        null,
                        null,
                        null,
                        100,
                        300,
                        Collections.emptyList(),
                        new OpcSecurity("None", "None", OpcSecurity.ANONYMOUS));
        Map<String, AtomicInteger> sent = new ConcurrentHashMap<>();
        when(transport.sendRequestMessage(any()))
                .thenAnswer(
                        call -> {
                            UaRequestMessageType request = call.getArgument(0);
                            sent.computeIfAbsent(
                                            request.getClass().getSimpleName(),
                                            k -> new AtomicInteger())
                                    .incrementAndGet();
                            try {
                                return CompletableFuture.completedFuture(
                                        respond(request, endpoint, attempt, cancel));
                            } catch (UaException failure) {
                                return CompletableFuture.failedFuture(failure);
                            }
                        });
        BoundedOpcUaClient client =
                new BoundedOpcUaClient(
                        OpcConnection.clientConfig(
                                settings,
                                endpoint,
                                List.of(endpoint),
                                null,
                                OpcConnection.rejectUnconfiguredTrust()),
                        transport,
                        attempt);
        try {
            CompletableFuture<?> connected = assertDoesNotThrow(client::connectAsync);
            try {
                connected.get(3, TimeUnit.SECONDS);
            } catch (ExecutionException | CancellationException failure) {
                assertTrue(cancel, "The uncancelled session must connect successfully");
            }
            if (cancel) {
                assertThrows(CancellationException.class, () -> attempt.await(connected, 1000));
                assertEquals(0, sent.getOrDefault("ReadRequest", new AtomicInteger()).get());
            } else {
                assertFalse(attempt.isCancelled());
                assertTrue(
                        sent.getOrDefault("ReadRequest", new AtomicInteger()).get() > 0,
                        "The control must exercise Milo's NamespaceArray initializer");
            }
            client.disconnectAsync().get(3, TimeUnit.SECONDS);
            assertEquals(1, sent.getOrDefault("CloseSessionRequest", new AtomicInteger()).get());
            verify(transport, atLeastOnce()).disconnect();
            verify(transport, times(1)).sendRequestMessage(any(ActivateSessionRequest.class));
        } finally {
            try {
                client.disconnectAsync().get(3, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
            }
        }
    }

    private static UaResponseMessageType respond(
            UaRequestMessageType request,
            EndpointDescription endpoint,
            ConnectionAttempt attempt,
            boolean cancel)
            throws UaException {
        ResponseHeader header =
                new ResponseHeader(
                        DateTime.now(), uint(0), StatusCode.GOOD, null, new String[0], null);
        if (request instanceof CreateSessionRequest)
            return new CreateSessionResponse(
                    header,
                    new NodeId(1, "session"),
                    new NodeId(1, "token"),
                    60000.0,
                    ByteString.of(new byte[32]),
                    ByteString.NULL_VALUE,
                    new EndpointDescription[] {endpoint},
                    new SignedSoftwareCertificate[0],
                    new SignatureData(null, null),
                    uint(0));
        if (request instanceof ActivateSessionRequest) {
            if (cancel) attempt.cancel();
            return new ActivateSessionResponse(
                    header, ByteString.of(new byte[32]), new StatusCode[0], new DiagnosticInfo[0]);
        }
        if (request instanceof ReadRequest) {
            ReadValueId[] nodes = ((ReadRequest) request).getNodesToRead();
            DataValue[] values = new DataValue[nodes.length];
            for (int i = 0; i < nodes.length; i++)
                values[i] =
                        new DataValue(
                                new Variant(
                                        new String[] {"http://opcfoundation.org/UA/", "urn:test"}));
            return new ReadResponse(header, values, new DiagnosticInfo[0]);
        }
        if (request instanceof CloseSessionRequest) return new CloseSessionResponse(header);
        throw new UaException(
                StatusCodes.Bad_ServiceUnsupported, request.getClass().getSimpleName());
    }
}
