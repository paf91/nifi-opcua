/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.nifi.processor.ProcessContext;
import org.eclipse.milo.opcua.sdk.client.*;
import org.eclipse.milo.opcua.sdk.client.subscriptions.*;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.UaSerializationException;
import org.eclipse.milo.opcua.stack.core.UaServiceFaultException;
import org.eclipse.milo.opcua.stack.core.types.UaRequestMessageType;
import org.eclipse.milo.opcua.stack.core.types.UaResponseMessageType;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType;
import org.eclipse.milo.opcua.stack.core.types.structured.*;
import org.eclipse.milo.opcua.stack.transport.client.OpcClientTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import java.util.stream.Stream;

class BoundedMonitoredItemSetupTest {
    private final ConnectionAttempt attempt = new ConnectionAttempt();
    private final OpcClientTransport transport = mock(OpcClientTransport.class, RETURNS_DEEP_STUBS);
    private final List<Integer> requestSizes = new CopyOnWriteArrayList<>();
    private BoundedOpcUaClient client;
    private OpcUaSession session;
    private OpcUaSubscription subscription;

    @BeforeEach
    void setup() throws Exception {
        EndpointDescription endpoint =
                OpcSecurityTest.endpoint(
                        "None", "None", OpcSecurityTest.token(UserTokenType.Anonymous, null));
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
        when(transport.getConfig().getExecutor()).thenReturn(mock(ExecutorService.class));
        client =
                spy(
                        new BoundedOpcUaClient(
                                OpcConnection.clientConfig(
                                        settings,
                                        endpoint,
                                        List.of(endpoint),
                                        null,
                                        OpcConnection.rejectUnconfiguredTrust()),
                                transport,
                                attempt));
        session = mock(OpcUaSession.class);
        when(session.getAuthenticationToken()).thenReturn(new NodeId(0, 1));
        doReturn(CompletableFuture.completedFuture(session)).when(client).getSessionAsync();
        OperationLimits limits = mock(OperationLimits.class);
        when(limits.maxMonitoredItemsPerCall()).thenReturn(Optional.of(uint(50)));
        doReturn(limits).when(client).getOperationLimits();
        doReturn(
                        CompletableFuture.completedFuture(
                                new CreateSubscriptionResponse(
                                        null, uint(1), 1000.0, uint(60), uint(20))))
                .when(client)
                .createSubscriptionAsync(anyDouble(), any(), any(), any(), anyBoolean(), any());
        subscription = new OpcUaSubscription(client, 1000);
        subscription.setMaxMonitoredItemsPerCall(uint(100));
        subscription.createAsync().toCompletableFuture().get(1, TimeUnit.SECONDS);
    }

    private Map<NodeId, OpcUaMonitoredItem> addItems(int count) {
        Map<NodeId, OpcUaMonitoredItem> items = new HashMap<>();
        for (int i = 0; i < count; i++) {
            NodeId node = new NodeId(2, "tag" + i);
            OpcUaMonitoredItem item = OpcUaMonitoredItem.newDataItem(node);
            subscription.addMonitoredItem(item);
            items.put(node, item);
        }
        return items;
    }

    private void respondToCreates(
            Function<
                            CreateMonitoredItemsRequest,
                            CompletableFuture<? extends UaResponseMessageType>>
                    responder) {
        when(transport.sendRequestMessage(any(CreateMonitoredItemsRequest.class)))
                .thenAnswer(
                        call -> {
                            CreateMonitoredItemsRequest request = call.getArgument(0);
                            requestSizes.add(request.getItemsToCreate().length);
                            return responder.apply(request);
                        });
    }

    private static ResponseHeader responseHeader(UInteger requestHandle, StatusCode status) {
        return new ResponseHeader(DateTime.now(), requestHandle, status, null, new String[0], null);
    }

    private static UaServiceFaultException serviceFault(
            CreateMonitoredItemsRequest request, long status) {
        return serviceFault(request.getRequestHeader().getRequestHandle(), status);
    }

    private static UaServiceFaultException serviceFault(UInteger requestHandle, long status) {
        return new UaServiceFaultException(
                new ServiceFault(responseHeader(requestHandle, new StatusCode(status))));
    }

    private static UaException outboundSizeFailure() {
        return new UaException(
                new UaSerializationException(
                        StatusCodes.Bad_RequestTooLarge, "outbound request exceeds limit"));
    }

    private static MonitoredItemCreateResult acceptedResult(UInteger handle) {
        long value = handle.longValue();
        return new MonitoredItemCreateResult(
                StatusCode.GOOD, uint(10000 + value), 1000.0 + value, uint(10 + value), null);
    }

    private static CreateMonitoredItemsResponse response(
            CreateMonitoredItemsRequest request, MonitoredItemCreateResult... results) {
        return new CreateMonitoredItemsResponse(
                responseHeader(request.getRequestHeader().getRequestHandle(), StatusCode.GOOD),
                results,
                new DiagnosticInfo[0]);
    }

    private static CreateMonitoredItemsResponse accepted(CreateMonitoredItemsRequest request) {
        MonitoredItemCreateResult[] results =
                new MonitoredItemCreateResult[request.getItemsToCreate().length];
        for (int i = 0; i < results.length; i++)
            results[i] =
                    acceptedResult(
                            request.getItemsToCreate()[i]
                                    .getRequestedParameters()
                                    .getClientHandle());
        return response(request, results);
    }

    private static CreateMonitoredItemsResponse acceptOnce(
            CreateMonitoredItemsRequest request, Map<NodeId, UInteger> acceptedHandles) {
        for (MonitoredItemCreateRequest item : request.getItemsToCreate()) {
            NodeId node = item.getItemToMonitor().getNodeId();
            assertNull(
                    acceptedHandles.put(node, item.getRequestedParameters().getClientHandle()),
                    () -> "Duplicate create for " + node);
        }
        return accepted(request);
    }

    private void assertSynchronizedItems(
            Map<NodeId, OpcUaMonitoredItem> configured, Map<NodeId, UInteger> acceptedHandles) {
        assertTrue(subscription.isMonitoredItemsSynchronized());
        assertEquals(configured.keySet(), acceptedHandles.keySet());
        assertEquals(configured.size(), new HashSet<>(acceptedHandles.values()).size());
        assertEquals(configured.size(), subscription.getMonitoredItems().size());
        for (OpcUaMonitoredItem item : subscription.getMonitoredItems()) {
            NodeId node = item.getReadValueId().getNodeId();
            assertSame(configured.get(node), item);
            UInteger handle = acceptedHandles.get(node);
            assertEquals(handle, item.getClientHandle().orElseThrow());
            MonitoredItemCreateResult expected = acceptedResult(handle);
            assertEquals(expected.getStatusCode(), item.getCreateResult().orElseThrow());
            assertEquals(expected.getMonitoredItemId(), item.getMonitoredItemId().orElseThrow());
            assertEquals(
                    expected.getRevisedSamplingInterval(),
                    item.getRevisedSamplingInterval().orElseThrow());
            assertEquals(expected.getRevisedQueueSize(), item.getRevisedQueueSize().orElseThrow());
        }
    }

    @Test
    void lostSessionIsBoundedAndLateRecoveryDoesNotSendTheAbandonedRequest() {
        addItems(201);
        CompletableFuture<OpcUaSession> recovery = new CompletableFuture<>();
        doReturn(recovery).when(client).getSessionAsync();
        Exception failure =
                assertTimeoutPreemptively(
                        Duration.ofSeconds(2),
                        () ->
                                assertThrows(
                                        Exception.class,
                                        () ->
                                                OpcConnection.synchronizeMonitoredItems(
                                                        subscription, attempt)));
        assertTrue(hasTimeout(failure), failure.toString());
        assertFalse(recovery.isCancelled());
        recovery.complete(session);
        verify(transport, never()).sendRequestMessage(any(CreateMonitoredItemsRequest.class));
    }

    @Test
    void lazyOperationLimitReadAlsoHasADeadlineIncludingSessionAcquisition() throws Exception {
        addItems(1);
        doCallRealMethod().when(client).getOperationLimits();
        CompletableFuture<OpcUaSession> recovery = new CompletableFuture<>();
        doReturn(recovery).when(client).getSessionAsync();
        Exception failure =
                assertTimeoutPreemptively(
                        Duration.ofSeconds(2),
                        () ->
                                assertThrows(
                                        Exception.class,
                                        () ->
                                                OpcConnection.synchronizeMonitoredItems(
                                                        subscription, attempt)));
        assertTrue(hasTimeout(failure), failure.toString());
        recovery.complete(session);
        verify(transport, never()).sendRequestMessage(any(ReadRequest.class));
        verify(transport, never()).sendRequestMessage(any(CreateMonitoredItemsRequest.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellationStopsAtThePendingBatchAndClearsItsInterrupt(boolean shrink) throws Exception {
        addItems(201);
        CountDownLatch sent = new CountDownLatch(1);
        respondToCreates(
                request -> {
                    if (shrink && requestSizes.size() == 1)
                        return CompletableFuture.failedFuture(
                                serviceFault(request, StatusCodes.Bad_TooManyOperations));
                    sent.countDown();
                    return new CompletableFuture<UaResponseMessageType>();
                });
        ExecutorService worker = Executors.newSingleThreadExecutor();
        AtomicBoolean interruptedAfterwards = new AtomicBoolean(true);
        try {
            Future<?> setup =
                    worker.submit(
                            () -> {
                                try {
                                    OpcConnection.synchronizeMonitoredItems(subscription, attempt);
                                } finally {
                                    interruptedAfterwards.set(
                                            Thread.currentThread().isInterrupted());
                                }
                            });
            assertTrue(sent.await(2, TimeUnit.SECONDS));
            attempt.cancel();
            assertThrows(ExecutionException.class, () -> setup.get(2, TimeUnit.SECONDS));
            assertEquals(shrink ? List.of(50, 25) : List.of(50), requestSizes);
            assertFalse(interruptedAfterwards.get());
        } finally {
            attempt.cancel();
            worker.shutdownNow();
        }
    }

    @Test
    void serviceResponseTimeoutAbortsBeforeTheNextPartition() {
        addItems(201);
        when(transport.sendRequestMessage(any(CreateMonitoredItemsRequest.class)))
                .thenReturn(new CompletableFuture<>());
        Exception failure =
                assertTimeoutPreemptively(
                        Duration.ofSeconds(2),
                        () ->
                                assertThrows(
                                        Exception.class,
                                        () ->
                                                OpcConnection.synchronizeMonitoredItems(
                                                        subscription, attempt)));
        assertTrue(hasTimeout(failure), failure.toString());
        verify(transport, times(1)).sendRequestMessage(any(CreateMonitoredItemsRequest.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void eachAcceptedBatchHasItsOwnDeadline(boolean shrink) {
        addItems(shrink ? 8 : 201);
        ScheduledExecutorService replies = Executors.newSingleThreadScheduledExecutor();
        respondToCreates(
                request -> {
                    if (shrink && request.getItemsToCreate().length > 2)
                        return CompletableFuture.failedFuture(
                                serviceFault(request, StatusCodes.Bad_TooManyOperations));
                    CompletableFuture<UaResponseMessageType> response = new CompletableFuture<>();
                    replies.schedule(
                            () -> response.complete(accepted(request)), 100, TimeUnit.MILLISECONDS);
                    return response;
                });
        try {
            assertDoesNotThrow(
                    () -> OpcConnection.synchronizeMonitoredItems(subscription, attempt));
            assertEquals(
                    shrink ? List.of(8, 4, 2, 2, 2, 2) : List.of(50, 50, 50, 50, 1), requestSizes);
            assertTrue(subscription.isMonitoredItemsSynchronized());
        } finally {
            replies.shutdownNow();
        }
    }

    static Stream<Arguments> serviceLimits() {
        return Stream.of(
                Arguments.of(100, StatusCodes.Bad_TooManyOperations),
                Arguments.of(50, StatusCodes.Bad_TooManyOperations),
                Arguments.of(50, StatusCodes.Bad_RequestTooLarge));
    }

    @ParameterizedTest
    @MethodSource("serviceLimits")
    void createsOneThousandItemsAtTheAcceptedBatchSize(int limit, long status) throws UaException {
        when(client.getOperationLimits().maxMonitoredItemsPerCall())
                .thenReturn(Optional.of(uint(100)));
        var configured = addItems(1000);
        Map<NodeId, UInteger> acceptedHandles = new HashMap<>();
        respondToCreates(
                request -> {
                    if (request.getItemsToCreate().length > limit)
                        return CompletableFuture.failedFuture(serviceFault(request, status));
                    return CompletableFuture.completedFuture(acceptOnce(request, acceptedHandles));
                });
        OpcConnection.synchronizeMonitoredItems(subscription, attempt);
        assertSynchronizedItems(configured, acceptedHandles);
        List<Integer> expected = new ArrayList<>(Collections.nCopies(1000 / limit, limit));
        if (limit < 100) expected.add(0, 100);
        assertEquals(expected.size(), requestSizes.size());
        assertEquals(expected, requestSizes);
    }

    @Test
    void smallerEncodingLimitAfterAnAcceptedPrefixKeepsEveryResultOnItsOriginalItem() {
        var configured = addItems(30);
        Map<NodeId, UInteger> acceptedHandles = new HashMap<>();
        respondToCreates(
                request -> {
                    int size = request.getItemsToCreate().length;
                    for (MonitoredItemCreateRequest item : request.getItemsToCreate()) {
                        NodeId node = item.getItemToMonitor().getNodeId();
                        assertFalse(acceptedHandles.containsKey(node), () -> "Replayed " + node);
                    }
                    if (size > (acceptedHandles.isEmpty() ? 15 : 7)) {
                        return CompletableFuture.failedFuture(outboundSizeFailure());
                    }
                    return CompletableFuture.completedFuture(acceptOnce(request, acceptedHandles));
                });
        OpcConnection.synchronizeMonitoredItems(subscription, attempt);
        assertSynchronizedItems(configured, acceptedHandles);
        assertEquals(List.of(30, 15, 15, 7, 7, 1), requestSizes);
    }

    private enum CreateLimit {
        SERVER_OPERATION_COUNT,
        SERVER_REQUEST_SIZE,
        OUTBOUND_REQUEST_SIZE
    }

    static Stream<Arguments> singletonLimits() {
        return Stream.of(
                Arguments.of(CreateLimit.SERVER_OPERATION_COUNT, false),
                Arguments.of(CreateLimit.SERVER_REQUEST_SIZE, true),
                Arguments.of(CreateLimit.OUTBOUND_REQUEST_SIZE, false),
                Arguments.of(CreateLimit.OUTBOUND_REQUEST_SIZE, true));
    }

    @ParameterizedTest
    @MethodSource("singletonLimits")
    void singletonRejectionReportsTheNodeAndOriginalStatus(
            CreateLimit limit, boolean oversizedNode) {
        long status =
                limit == CreateLimit.SERVER_OPERATION_COUNT
                        ? StatusCodes.Bad_TooManyOperations
                        : StatusCodes.Bad_RequestTooLarge;
        String prefix = oversizedNode ? "oversized".repeat(256) : "tag";
        for (int i = 0; i < 3; i++)
            subscription.addMonitoredItem(
                    OpcUaMonitoredItem.newDataItem(new NodeId(2, prefix + i)));
        AtomicReference<NodeId> rejectedNode = new AtomicReference<>();
        AtomicReference<UaException> original = new AtomicReference<>();
        respondToCreates(
                request -> {
                    UaException fault =
                            limit == CreateLimit.OUTBOUND_REQUEST_SIZE
                                    ? outboundSizeFailure()
                                    : serviceFault(request, status);
                    if (request.getItemsToCreate().length == 1) {
                        rejectedNode.set(
                                request.getItemsToCreate()[0].getItemToMonitor().getNodeId());
                        original.set(fault);
                    }
                    return CompletableFuture.failedFuture(fault);
                });
        Exception failure = assertFailureStatus(status);
        assertEquals(List.of(3, 1), requestSizes);
        assertSame(original.get(), UaException.extract(failure).orElseThrow());
        var runner =
                ProcessorTest.configured(
                        new ConsumeOPCUA() {
                            @Override
                            protected AutoCloseable openConnection(
                                    ProcessContext context, ConnectionAttempt attempt)
                                    throws Exception {
                                throw failure;
                            }
                        });
        try {
            runner.run(1, false, true);
            var errors = runner.getLogger().getErrorMessages();
            assertEquals(1, errors.size());
            String log = errors.get(0).toString();
            assertTrue(log.contains(StatusCodes.lookup(status).orElseThrow()[0]), log);
            assertTrue(log.contains(String.format(Locale.ROOT, "0x%08X", status)), log);
            if (limit == CreateLimit.OUTBOUND_REQUEST_SIZE)
                assertTrue(log.contains(UaSerializationException.class.getSimpleName()), log);
            NodeId node = rejectedNode.get();
            assertTrue(
                    log.contains(
                            oversizedNode
                                    ? "namespace " + node.getNamespaceIndex()
                                    : node.toParseableString()),
                    log);
            if (oversizedNode) assertFalse(log.contains(prefix));
        } finally {
            runner.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                StatusCodes.Bad_TooManyOperations,
                StatusCodes.Bad_RequestTooLarge,
                StatusCodes.Bad_Timeout,
                StatusCodes.Bad_ResponseTooLarge
            })
    void abortsAfterOneUnconfirmedTransportFailure(long status) {
        addItems(101);
        when(transport.sendRequestMessage(any(CreateMonitoredItemsRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(new UaException(status)));
        assertFailureStatus(status);
        verify(transport, times(1)).sendRequestMessage(any(CreateMonitoredItemsRequest.class));
    }

    @ParameterizedTest
    @ValueSource(longs = {StatusCodes.Bad_RequestTooLarge, StatusCodes.Bad_TooManyOperations})
    void aServiceFaultForAnotherRequestCannotAuthorizeACreateRetry(long status) {
        addItems(101);
        respondToCreates(
                request -> {
                    long handle = request.getRequestHeader().getRequestHandle().longValue();
                    return CompletableFuture.failedFuture(serviceFault(uint(handle + 1), status));
                });
        assertFailureStatus(status);
        assertEquals(List.of(50), requestSizes);
    }

    @ParameterizedTest
    @ValueSource(longs = {StatusCodes.Bad_RequestTooLarge, StatusCodes.Bad_TooManyOperations})
    void aSessionAcquisitionFaultCannotAuthorizeACreateRetry(long status) {
        addItems(101);
        doReturn(CompletableFuture.failedFuture(serviceFault(uint(1), status)))
                .when(client)
                .getSessionAsync();
        assertFailureStatus(status);
        verify(client).getSessionAsync();
        verify(transport, never()).sendRequestMessage(any(CreateMonitoredItemsRequest.class));
    }

    @ParameterizedTest
    @ValueSource(longs = {StatusCodes.Bad_Timeout, StatusCodes.Bad_ResponseTooLarge})
    void ambiguousServiceFaultStopsAfterOneRequest(long status) {
        addItems(101);
        respondToCreates(request -> CompletableFuture.failedFuture(serviceFault(request, status)));
        assertFailureStatus(status);
        assertEquals(List.of(50), requestSizes);
    }

    private Exception assertFailureStatus(long status) {
        Exception failure =
                assertThrows(
                        Exception.class,
                        () -> OpcConnection.synchronizeMonitoredItems(subscription, attempt));
        assertTrue(
                failure.getMessage().contains(StatusCodes.lookup(status).orElseThrow()[0]),
                failure.toString());
        return failure;
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                StatusCodes.Bad_TooManyOperations,
                StatusCodes.Bad_RequestTooLarge,
                StatusCodes.Uncertain_LastUsableValue
            })
    void reportsPerItemFailureAndStopsBeforeTheNextReducedBatch(long status) {
        addItems(101);
        AtomicReference<NodeId> rejectedNode = new AtomicReference<>();
        respondToCreates(
                request -> {
                    if (requestSizes.size() == 1)
                        return CompletableFuture.failedFuture(
                                serviceFault(request, StatusCodes.Bad_TooManyOperations));
                    CreateMonitoredItemsResponse response = accepted(request);
                    if (requestSizes.size() == 3) {
                        int last = response.getResults().length - 1;
                        rejectedNode.set(
                                request.getItemsToCreate()[last].getItemToMonitor().getNodeId());
                        response.getResults()[last] =
                                new MonitoredItemCreateResult(
                                        new StatusCode(status), uint(0), 0.0, uint(0), null);
                    }
                    return CompletableFuture.completedFuture(response);
                });
        Exception failure = assertFailureStatus(status);
        assertEquals(List.of(50, 25, 25), requestSizes);
        assertTrue(
                failure.getMessage().contains(rejectedNode.get().getIdentifier().toString()),
                failure.toString());
    }

    private enum MalformedReply {
        MISSING_RESPONSE,
        MISSING_RESULTS,
        TOO_FEW_RESULTS,
        TOO_MANY_RESULTS,
        MISSING_RESULT,
        MISSING_STATUS
    }

    @ParameterizedTest
    @EnumSource(MalformedReply.class)
    void abortsMalformedCreateBeforeAnotherRequest(MalformedReply malformed) {
        subscription.setMaxMonitoredItemsPerCall(uint(2));
        addItems(6);
        respondToCreates(
                request -> {
                    var results = accepted(request).getResults();
                    return CompletableFuture.completedFuture(
                            switch (malformed) {
                                case MISSING_RESPONSE -> null;
                                case MISSING_RESULTS ->
                                        response(request, (MonitoredItemCreateResult[]) null);
                                case TOO_FEW_RESULTS -> response(request, results[0]);
                                case TOO_MANY_RESULTS ->
                                        response(
                                                request,
                                                results[0],
                                                results[1],
                                                acceptedResult(uint(1234)));
                                case MISSING_RESULT -> response(request, results[0], null);
                                case MISSING_STATUS ->
                                        response(
                                                request,
                                                results[0],
                                                new MonitoredItemCreateResult(
                                                        null, uint(2), 100.0, uint(1), null));
                            });
                });
        assertThrows(
                Exception.class,
                () -> OpcConnection.synchronizeMonitoredItems(subscription, attempt));
        assertEquals(List.of(2), requestSizes);
    }

    @Test
    void sendBoundaryRejectsLateSetupButStillAllowsCleanupAfterCancellation() {
        attempt.cancel();
        for (UaRequestMessageType request :
                new UaRequestMessageType[] {
                    new CreateMonitoredItemsRequest(
                            null,
                            uint(1),
                            org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn
                                    .Both,
                            new MonitoredItemCreateRequest[0]),
                    new ReadRequest(
                            null,
                            0.0,
                            org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn
                                    .Both,
                            new ReadValueId[0])
                }) {
            CompletableFuture<UaResponseMessageType> response =
                    assertDoesNotThrow(() -> client.sendRequestAsync(request));
            assertTrue(response.isDone());
            assertThrows(CancellationException.class, response::join);
        }
        verify(transport, never()).sendRequestMessage(any(CreateMonitoredItemsRequest.class));
        verify(transport, never()).sendRequestMessage(any(ReadRequest.class));
        DeleteSubscriptionsRequest cleanup =
                new DeleteSubscriptionsRequest(
                        null,
                        new org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger[] {
                            uint(1)
                        });
        when(transport.sendRequestMessage(cleanup))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new DeleteSubscriptionsResponse(
                                        null,
                                        new StatusCode[] {StatusCode.GOOD},
                                        new DiagnosticInfo[0])));
        assertDoesNotThrow(() -> client.sendRequestAsync(cleanup).join());
        verify(transport).sendRequestMessage(cleanup);
    }

    private static boolean hasTimeout(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof TimeoutException) return true;
        return false;
    }

    @Test
    void recreatedSubscriptionUsesMilosNativeFallbackAfterSetup() throws Exception {
        addItems(12);
        AtomicInteger limit = new AtomicInteger(6);
        respondToCreates(
                request -> {
                    if (request.getItemsToCreate().length > limit.get())
                        return CompletableFuture.failedFuture(
                                serviceFault(request, StatusCodes.Bad_TooManyOperations));
                    return CompletableFuture.completedFuture(accepted(request));
                });
        OpcConnection.synchronizeMonitoredItems(subscription, attempt);
        client.setupComplete();
        subscription.reset();
        subscription.createAsync().toCompletableFuture().get(1, TimeUnit.SECONDS);
        requestSizes.clear();
        limit.set(2);
        subscription.synchronizeMonitoredItems();
        assertTrue(subscription.isMonitoredItemsSynchronized());
        assertTrue(requestSizes.get(0) > limit.get());
        assertEquals(Collections.nCopies(12, 1), requestSizes.subList(1, requestSizes.size()));
    }

    @Test
    void completedSetupPreservesSdkRecoveryErrorsButStillHonorsCancellation() {
        client.setupComplete();
        when(transport.sendRequestMessage(any(CreateMonitoredItemsRequest.class)))
                .thenReturn(
                        CompletableFuture.failedFuture(new UaException(StatusCodes.Bad_Timeout)));
        UaException failure =
                assertThrows(
                        UaException.class,
                        () ->
                                client.createMonitoredItems(
                                        uint(1),
                                        org.eclipse.milo.opcua.stack.core.types.enumerated
                                                .TimestampsToReturn.Both,
                                        Collections.emptyList()));
        assertEquals(StatusCodes.Bad_Timeout, failure.getStatusCode().getValue());
        attempt.cancel();
        assertThrows(
                CancellationException.class,
                () ->
                        client.createMonitoredItems(
                                uint(1),
                                org.eclipse.milo.opcua.stack.core.types.enumerated
                                        .TimestampsToReturn.Both,
                                Collections.emptyList()));
        verify(transport, times(1)).sendRequestMessage(any(CreateMonitoredItemsRequest.class));
    }
}
