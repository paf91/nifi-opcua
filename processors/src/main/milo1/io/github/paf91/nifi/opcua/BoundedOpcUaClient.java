/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.OpcUaClientConfig;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.UaSerializationException;
import org.eclipse.milo.opcua.stack.core.UaServiceFaultException;
import org.eclipse.milo.opcua.stack.core.types.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.*;
import org.eclipse.milo.opcua.stack.transport.client.OpcClientTransport;

import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Retains Milo's subscription bookkeeping and batching while bounding its blocking setup calls. */
class BoundedOpcUaClient extends OpcUaClient {
    private final ConnectionAttempt attempt;
    private final long timeout;
    private volatile boolean initialSetup = true;

    BoundedOpcUaClient(
            OpcUaClientConfig config, OpcClientTransport transport, ConnectionAttempt attempt) {
        super(config, transport);
        this.attempt = attempt;
        timeout = config.getRequestTimeout().longValue();
    }

    void setupComplete() {
        initialSetup = false;
    }

    @Override
    public CreateMonitoredItemsResponse createMonitoredItems(
            UInteger subscriptionId,
            TimestampsToReturn timestamps,
            List<MonitoredItemCreateRequest> requests)
            throws UaException {
        attempt.check();
        // After initial setup, leave automatic subscription recovery's error handling to Milo.
        if (!initialSetup) return super.createMonitoredItems(subscriptionId, timestamps, requests);
        CreateMonitoredItemsResponse response;
        try {
            response =
                    BoundedRequest.execute(
                            attempt,
                            timeout,
                            () ->
                                    super.createMonitoredItemsAsync(
                                            subscriptionId, timestamps, requests));
        } catch (UaException e) {
            // Leave Milo's partition loop before it sends another Create.
            if (requests.size() == 1)
                throw new IllegalStateException(
                        "OPC UA operation failed with "
                                + e.getStatusCode()
                                + " while creating monitored item "
                                + describeNode(requests.get(0).getItemToMonitor().getNodeId()),
                        e);
            throw new CompletionException(e);
        }
        var results = response == null ? null : response.getResults();
        if (results == null || results.length != requests.size())
            throw new IllegalStateException(
                    "CreateMonitoredItems returned "
                            + (results == null ? "no results" : results.length + " results")
                            + " for "
                            + requests.size()
                            + " items");
        for (int i = 0; i < results.length; i++) {
            StatusCode status = results[i] == null ? null : results[i].getStatusCode();
            if (status == null || !status.isGood())
                throw new IllegalStateException(
                        "CreateMonitoredItems returned "
                                + (status == null ? "no status" : status)
                                + " for "
                                + describeNode(requests.get(i).getItemToMonitor().getNodeId()));
        }
        return response;
    }

    private static String describeNode(NodeId node) {
        try {
            NotificationJson.checkNodeIdSize(node, 512);
            return node.toParseableString();
        } catch (NotificationJson.RecordTooLargeException e) {
            return "node in namespace "
                    + node.getNamespaceIndex()
                    + " (identifier too large to display)";
        }
    }

    @Override
    public List<DataValue> readValues(
            double maxAge, TimestampsToReturn timestamps, List<NodeId> nodes) throws UaException {
        attempt.check();
        if (!initialSetup) return super.readValues(maxAge, timestamps, nodes);
        // Milo lazily reads operation limits here, including its single-node fallback through
        // readValue(). Await the original read future so cancellation also fences session recovery.
        List<ReadValueId> requests = new ArrayList<>(nodes.size());
        for (NodeId node : nodes)
            requests.add(
                    new ReadValueId(node, AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE));
        ReadResponse response =
                BoundedRequest.execute(
                        attempt, timeout, () -> super.readAsync(maxAge, timestamps, requests));
        return Arrays.asList(response.getResults());
    }

    @Override
    public CompletableFuture<UaResponseMessageType> sendRequestAsync(UaRequestMessageType request) {
        // A session can arrive on another thread after Stop. Fence at the actual send boundary
        // as well as before each blocking operation. Keep deletion/close requests available.
        if (request instanceof CreateMonitoredItemsRequest || request instanceof ReadRequest) {
            try {
                var response = attempt.callIfActive(() -> super.sendRequestAsync(request));
                if (initialSetup
                        && request instanceof CreateMonitoredItemsRequest create
                        && create.getItemsToCreate().length > 1) {
                    return response.exceptionallyCompose(
                            error -> {
                                UaException failure = UaException.extract(error).orElse(null);
                                if (failure != null && rejectedBatch(create, failure))
                                    return CompletableFuture.failedFuture(
                                            new RejectedBatch(
                                                    create.getItemsToCreate().length, failure));
                                return CompletableFuture.failedFuture(error);
                            });
                }
                return response;
            } catch (CancellationException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        return super.sendRequestAsync(request);
    }

    private static boolean rejectedBatch(CreateMonitoredItemsRequest request, UaException failure) {
        long status = failure.getStatusCode().getValue();
        if (failure instanceof UaServiceFaultException fault) {
            UInteger handle = fault.getServiceFault().getResponseHeader().getRequestHandle();
            return (status == StatusCodes.Bad_TooManyOperations
                            || status == StatusCodes.Bad_RequestTooLarge)
                    && handle != null
                    && request.getRequestHeader() != null
                    && handle.equals(request.getRequestHeader().getRequestHandle());
        }
        // Milo raises this serialization error before writing any request chunks.
        return status == StatusCodes.Bad_RequestTooLarge
                && failure.getCause() instanceof UaSerializationException serialization
                && serialization.getStatusCode().getValue() == StatusCodes.Bad_RequestTooLarge;
    }

    static final class RejectedBatch extends UaException {
        final int nextSize;

        private RejectedBatch(int requestSize, UaException cause) {
            super(cause);
            nextSize = requestSize / 2;
        }
    }
}
