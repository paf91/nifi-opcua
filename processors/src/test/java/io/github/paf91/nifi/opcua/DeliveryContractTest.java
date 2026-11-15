/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.util.TestRunner;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class DeliveryContractTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TagDefinition TAG =
            TagDefinition.parse("test", ProcessorTest.NODE, 100, 10);

    @ParameterizedTest
    @ValueSource(strings = {"create", "write", "attributes", "provenance", "transfer", "commit"})
    void repositoryFailuresRetainTheOriginalBatch(String stage) throws Exception {
        ProcessorTest.OfflineProcessor p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        p.start(r.getProcessContext());
        try {
            p.acceptForTest(TAG, new DataValue(new Variant(71)));
            ProcessSession session = spy(r.getProcessSessionFactory().createSession());
            RuntimeException failure = new RuntimeException("injected " + stage + " failure");
            switch (stage) {
                case "create":
                    doThrow(failure).when(session).create();
                    break;
                case "write":
                    doThrow(failure)
                            .when(session)
                            .write(any(FlowFile.class), any(OutputStreamCallback.class));
                    break;
                case "attributes":
                    doThrow(failure).when(session).putAllAttributes(any(FlowFile.class), anyMap());
                    break;
                case "provenance":
                    doThrow(failure).when(session).getProvenanceReporter();
                    break;
                case "transfer":
                    doThrow(failure)
                            .when(session)
                            .transfer(any(FlowFile.class), any(Relationship.class));
                    break;
                case "commit":
                    doThrow(failure).when(session).commitAsync(any(Runnable.class), any());
                    break;
                default:
                    throw new AssertionError(stage);
            }
            assertThrows(RuntimeException.class, () -> p.onTrigger(r.getProcessContext(), session));
            verify(session).rollback();
            r.run(1, false, false);
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
            JsonNode records =
                    JSON.readTree(
                            r.getContentAsByteArray(
                                    r.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS).get(0)));
            assertEquals(71, records.get(0).get("value").intValue());
            r.run(1, false, false);
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
        } finally {
            p.stop();
        }
    }

    @Test
    void callbackReportedCommitFailureReleasesReservation() throws Exception {
        ProcessorTest.OfflineProcessor p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        p.start(r.getProcessContext());
        try {
            p.acceptForTest(TAG, new DataValue(new Variant(19)));
            ProcessSession session = spy(r.getProcessSessionFactory().createSession());
            AtomicReference<Consumer<Throwable>> onFailure = new AtomicReference<>();
            doAnswer(
                            call -> {
                                onFailure.set(call.getArgument(1));
                                return null;
                            })
                    .when(session)
                    .commitAsync(any(Runnable.class), any());
            p.onTrigger(r.getProcessContext(), session);
            ProcessSession blocked = spy(r.getProcessSessionFactory().createSession());
            p.onTrigger(r.getProcessContext(), blocked);
            verify(blocked, never()).create();
            session.rollback();
            onFailure.get().accept(new RuntimeException("injected commit callback failure"));
            r.run(1, false, false);
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
        } finally {
            p.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rollbackThenFailureCallbackThenRethrowSettlesOnceAndReplaysInOrder(boolean diagnostics)
            throws Exception {
        ProcessorTest.OfflineProcessor p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        if (diagnostics) r.setProperty(ConsumeOPCUA.DIAGNOSTICS_INTERVAL, "3600");
        p.start(r.getProcessContext());
        try {
            offerValues(p, 71, 82, 93);
            ProcessSession session = spy(r.getProcessSessionFactory().createSession());
            IllegalStateException failure = new IllegalStateException("repository commit failed");
            List<Boolean> reservationStates = new ArrayList<>();
            doAnswer(
                            call -> {
                                Consumer<Throwable> onFailure = call.getArgument(1);
                                reservationStates.add((Boolean) buffer(p, r).get("reserved"));
                                session.rollback();
                                reservationStates.add((Boolean) buffer(p, r).get("reserved"));
                                onFailure.accept(failure);
                                reservationStates.add((Boolean) buffer(p, r).get("reserved"));
                                throw failure;
                            })
                    .when(session)
                    .commitAsync(any(Runnable.class), any());
            ProcessException thrown =
                    assertThrows(
                            ProcessException.class,
                            () ->
                                    p.onTrigger(
                                            r.getProcessContext(),
                                            (ProcessSessionFactory) () -> session));
            assertSame(failure, thrown.getCause());
            assertEquals(Arrays.asList(true, true, false), reservationStates);
            org.mockito.InOrder order = inOrder(session);
            order.verify(session).commitAsync(any(Runnable.class), any());
            order.verify(session).rollback();
            order.verify(session).rollback(true);
            verify(session, times(1)).rollback();
            verify(session, times(1)).rollback(true);
            assertEquals(1, r.getLogger().getErrorMessages().size());
            assertEquals(3, buffer(p, r).get("records"));
            assertEquals(false, buffer(p, r).get("reserved"));
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 0);
            r.assertTransferCount(ConsumeOPCUA.DIAGNOSTICS, 0);
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            assertEquals(
                    Arrays.asList(71, 82, 93), OutputOptionsTest.values(r, ConsumeOPCUA.SUCCESS));
            r.assertTransferCount(ConsumeOPCUA.DIAGNOSTICS, diagnostics ? 1 : 0);
            assertEquals(0, buffer(p, r).get("records"));
            assertEquals(false, buffer(p, r).get("reserved"));
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
            r.assertTransferCount(ConsumeOPCUA.DIAGNOSTICS, diagnostics ? 1 : 0);
        } finally {
            p.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "property",
                "create-error",
                "write-error",
                "runtime",
                "diagnostic-create-error",
                "diagnostic-write-error"
            })
    void sessionFactoryTriggerFailuresRetainOrderedRecordsAndReleaseReservations(String stage)
            throws Exception {
        ProcessorTest.OfflineProcessor p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        boolean diagnostic = stage.startsWith("diagnostic-");
        if (diagnostic) r.setProperty(ConsumeOPCUA.DIAGNOSTICS_INTERVAL, "3600");
        p.start(r.getProcessContext());
        try {
            offerValues(p, 71, 82, 93);
            ProcessContext context = spy(r.getProcessContext());
            ProcessSession session = spy(r.getProcessSessionFactory().createSession());
            Throwable marker =
                    stage.endsWith("error")
                            ? new AssertionError("injected " + stage)
                            : new IllegalStateException("injected " + stage);
            if (stage.equals("property")) {
                doThrow(marker).when(context).getProperty(ConsumeOPCUA.DIAGNOSTICS_INTERVAL);
            } else if (stage.contains("write")) {
                doThrow(marker)
                        .when(session)
                        .write(any(FlowFile.class), any(OutputStreamCallback.class));
            } else {
                doThrow(marker).when(session).create();
            }
            ProcessSessionFactory factory = () -> session;
            Throwable thrown = assertThrows(Throwable.class, () -> p.onTrigger(context, factory));
            if (marker instanceof Error) assertSame(marker, thrown);
            else assertTrue(hasCause(thrown, marker), "The original failure must be retained");
            Map<?, ?> retained = buffer(p, r);
            assertEquals(3, retained.get("records"));
            assertEquals(false, retained.get("reserved"));
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            assertEquals(
                    Arrays.asList(71, 82, 93), OutputOptionsTest.values(r, ConsumeOPCUA.SUCCESS));
            assertEquals(0, buffer(p, r).get("records"));
            assertEquals(false, buffer(p, r).get("reserved"));
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
            r.assertTransferCount(ConsumeOPCUA.DIAGNOSTICS, diagnostic ? 1 : 0);
        } finally {
            p.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void synchronousRollbackKeepsOwnershipUntilCleanupFinishes(boolean rollbackFails)
            throws Exception {
        ProcessorTest.OfflineProcessor p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.STOP_DRAIN_TIMEOUT, "0");
        p.start(r.getProcessContext());
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch rollingBack = new CountDownLatch(1), finishRollback = new CountDownLatch(1);
        try {
            offerValues(p, 17, 28, 39);
            ProcessSession session = spy(r.getProcessSessionFactory().createSession());
            AssertionError marker = new AssertionError("session create marker");
            RuntimeException innerCleanup = new IllegalStateException("inner rollback marker");
            RuntimeException outerCleanup = new IllegalStateException("outer rollback marker");
            doThrow(marker).when(session).create();
            doAnswer(
                            call -> {
                                rollingBack.countDown();
                                assertTrue(finishRollback.await(5, TimeUnit.SECONDS));
                                call.callRealMethod();
                                if (rollbackFails) throw innerCleanup;
                                return null;
                            })
                    .when(session)
                    .rollback();
            if (rollbackFails) doThrow(outerCleanup).when(session).rollback(true);
            Future<Throwable> result =
                    worker.submit(
                            () -> {
                                try {
                                    p.onTrigger(
                                            r.getProcessContext(),
                                            (ProcessSessionFactory) () -> session);
                                    return null;
                                } catch (Throwable failure) {
                                    return failure;
                                }
                            });
            assertTrue(rollingBack.await(3, TimeUnit.SECONDS));
            assertEquals(true, buffer(p, r).get("reserved"));
            assertEquals(3, buffer(p, r).get("records"));
            finishRollback.countDown();
            Throwable thrown = result.get(5, TimeUnit.SECONDS);
            assertSame(marker, thrown);
            if (rollbackFails) {
                assertTrue(hasSuppressed(thrown, innerCleanup));
                assertTrue(hasSuppressed(thrown, outerCleanup));
            }
            assertEquals(false, buffer(p, r).get("reserved"));
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            assertEquals(
                    Arrays.asList(17, 28, 39), OutputOptionsTest.values(r, ConsumeOPCUA.SUCCESS));
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
            assertEquals(0, buffer(p, r).get("records"));
        } finally {
            finishRollback.countDown();
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(3, TimeUnit.SECONDS));
            p.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void submittedCallbacksKeepOwnershipAcrossWrapperFailureAndRestart(boolean success)
            throws Exception {
        ProcessorTest.OfflineProcessor p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.STOP_DRAIN_TIMEOUT, "0");
        r.setProperty(ConsumeOPCUA.DIAGNOSTICS_INTERVAL, "3600");
        r.setAllowSynchronousSessionCommits(true);
        p.start(r.getProcessContext());
        try {
            offerValues(p, 11, 22, 33);
            PendingCommit submitted = new PendingCommit(r);
            AssertionError marker = new AssertionError("wrapper commit marker");
            doAnswer(
                            call -> {
                                submitted.committed.set(call.getArgument(0));
                                submitted.failed.set(call.getArgument(1));
                                if (success) submitted.session.commit();
                                else submitted.session.rollback();
                                return null;
                            })
                    .when(submitted.session)
                    .commitAsync(any(Runnable.class), any());
            doThrow(marker).when(submitted.session).commitAsync();
            assertSame(
                    marker,
                    assertThrows(
                            AssertionError.class,
                            () ->
                                    p.onTrigger(
                                            r.getProcessContext(),
                                            (ProcessSessionFactory) () -> submitted.session)));
            verify(submitted.session).rollback(true);
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, success ? 1 : 0);
            assertPending(p, r, 3);
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            assertPending(p, r, 3);
            p.stop();
            p.start(r.getProcessContext());
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            assertPending(p, r, 3);
            if (success) submitted.committed.get().run();
            else
                submitted
                        .failed
                        .get()
                        .accept(new IllegalStateException("repository failed before callback"));
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            assertEquals(
                    Arrays.asList(11, 22, 33), OutputOptionsTest.values(r, ConsumeOPCUA.SUCCESS));
            assertEquals(0, buffer(p, r).get("records"));
            assertEquals(false, buffer(p, r).get("reserved"));
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
        } finally {
            p.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void repeatedCallbacksCannotSettleALaterAttempt(boolean measurements) throws Exception {
        ProcessorTest.OfflineProcessor p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.DIAGNOSTICS_INTERVAL, "3600");
        p.start(r.getProcessContext());
        try {
            if (measurements) offerValues(p, 14, 25, 36);
            PendingCommit old = new PendingCommit(r);
            p.onTrigger(r.getProcessContext(), (ProcessSessionFactory) () -> old.session);
            old.fail();
            PendingCommit current = new PendingCommit(r);
            p.onTrigger(r.getProcessContext(), (ProcessSessionFactory) () -> current.session);
            old.committed.get().run();
            old.failed.get().accept(new IllegalStateException("repeated old failure"));
            if (measurements) assertPending(p, r, 3);
            ProcessSession blocked = spy(r.getProcessSessionFactory().createSession());
            p.onTrigger(r.getProcessContext(), (ProcessSessionFactory) () -> blocked);
            verify(blocked, never()).create();
            current.fail();
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            assertEquals(
                    measurements ? Arrays.asList(14, 25, 36) : Collections.emptyList(),
                    OutputOptionsTest.values(r, ConsumeOPCUA.SUCCESS));
            r.assertTransferCount(ConsumeOPCUA.DIAGNOSTICS, 1);
            old.committed.get().run();
            current.failed.get().accept(new IllegalStateException("repeated current failure"));
            p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, measurements ? 1 : 0);
            r.assertTransferCount(ConsumeOPCUA.DIAGNOSTICS, 1);
            assertEquals(0, buffer(p, r).get("records"));
        } finally {
            p.stop();
        }
    }

    private static final class PendingCommit {
        final ProcessSession session;
        final AtomicReference<Runnable> committed = new AtomicReference<>();
        final AtomicReference<Consumer<Throwable>> failed = new AtomicReference<>();

        PendingCommit(TestRunner runner) {
            session = spy(runner.getProcessSessionFactory().createSession());
            doAnswer(
                            call -> {
                                committed.set(call.getArgument(0));
                                failed.set(call.getArgument(1));
                                return null;
                            })
                    .when(session)
                    .commitAsync(any(Runnable.class), any());
            doNothing().when(session).commitAsync();
        }

        void fail() {
            session.rollback();
            failed.get()
                    .accept(new IllegalStateException("repository rejected the submitted commit"));
        }
    }

    private static boolean hasCause(Throwable failure, Throwable expected) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            if (cause == expected) return true;
        return false;
    }

    private static boolean hasSuppressed(Throwable failure, Throwable expected) {
        for (Throwable suppressed : failure.getSuppressed())
            if (suppressed == expected || hasSuppressed(suppressed, expected)) return true;
        return false;
    }

    private static void offerValues(ConsumeOPCUA processor, int... values) {
        for (int value : values) processor.acceptForTest(TAG, new DataValue(new Variant(value)));
    }

    private static Map<?, ?> buffer(ConsumeOPCUA processor, TestRunner runner) {
        return (Map<?, ?>) processor.diagnosticSnapshot(runner.getProcessContext()).get("buffer");
    }

    private static void assertPending(ConsumeOPCUA processor, TestRunner runner, int records) {
        assertEquals(records, buffer(processor, runner).get("records"));
        assertEquals(true, buffer(processor, runner).get("reserved"));
    }

    @Test
    void randomizedBufferOperationsPreserveOrderAndReservations() {
        Random random = new Random(817432L);
        NotificationBuffer buffer = new NotificationBuffer(31);
        Deque<byte[]> expected = new ArrayDeque<>();
        List<byte[]> reserved = Collections.emptyList();
        for (int step = 0; step < 50000; step++) {
            switch (random.nextInt(4)) {
                case 0:
                    byte[] record = new byte[] {(byte) step};
                    boolean fits = expected.size() < 31;
                    assertEquals(fits, buffer.offer(record));
                    if (fits) expected.addLast(record);
                    break;
                case 1:
                    int maximum = 1 + random.nextInt(50);
                    List<byte[]> actual = buffer.reserve(maximum);
                    if (!reserved.isEmpty()) assertTrue(actual.isEmpty());
                    else {
                        List<byte[]> prefix = new ArrayList<>(expected);
                        prefix = prefix.subList(0, Math.min(maximum, prefix.size()));
                        assertEquals(prefix, actual);
                        reserved = actual;
                    }
                    break;
                case 2:
                    if (!reserved.isEmpty()) {
                        buffer.acknowledge(reserved.size());
                        for (byte[] ignored : reserved) expected.removeFirst();
                        reserved = Collections.emptyList();
                    }
                    break;
                case 3:
                    buffer.release();
                    reserved = Collections.emptyList();
                    break;
            }
            assertEquals(expected.size(), buffer.size());
            assertEquals(new ArrayList<>(expected), buffer.peek(100));
        }
    }

    @Test
    void preservesArrayIntegersBytesAndNonFiniteValues() throws Exception {
        assertEquals(
                "[\"-9223372036854775808\",\"9223372036854775807\"]",
                JSON.writeValueAsString(
                        NotificationJson.normalize(new long[] {Long.MIN_VALUE, Long.MAX_VALUE})));
        assertEquals(
                "[[1,2],[3,4]]",
                JSON.writeValueAsString(NotificationJson.normalize(new int[][] {{1, 2}, {3, 4}})));
        assertEquals(
                "[\"NaN\",\"Infinity\",\"-Infinity\"]",
                JSON.writeValueAsString(
                        NotificationJson.normalize(
                                new double[] {
                                    Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY
                                })));
        assertEquals("AQID", NotificationJson.normalize(new ByteString(new byte[] {1, 2, 3})));
        assertEquals("", NotificationJson.normalize(new ByteString(new byte[0])));
        assertNull(NotificationJson.normalize(new ByteString(null)));
    }

    @Test
    void preservesUncertainAndOverflowStatusBits() throws Exception {
        JsonNode uncertain =
                JSON.readTree(
                        NotificationJson.encode(
                                TAG,
                                new DataValue(
                                        new Variant(1), new StatusCode(0x40000000L), null, null)));
        assertEquals("Uncertain", uncertain.get("quality").textValue());
        JsonNode overflow =
                JSON.readTree(
                        NotificationJson.encode(
                                TAG,
                                new DataValue(
                                        new Variant(2), new StatusCode(0x00000480L), null, null)));
        assertEquals("0x00000480", overflow.get("statusCode").textValue());
        assertEquals("Good", overflow.get("quality").textValue());
        assertTrue(overflow.get("sourceTimestamp").isNull());
    }

    @Test
    void cancellationStopsPendingWaitAndFencesCallbacks() throws Exception {
        ConnectionAttempt attempt = new ConnectionAttempt();
        CompletableFuture<Object> response = new CompletableFuture<>();
        CountDownLatch entered = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> waiting =
                    worker.submit(
                            () -> {
                                entered.countDown();
                                assertThrows(
                                        CancellationException.class,
                                        () -> attempt.await(response, 300000));
                            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            attempt.cancel();
            waiting.get(5, TimeUnit.SECONDS);
            assertTrue(response.isCancelled());
            attempt.ifActive(() -> fail("Cancelled generation accepted a callback"));
        } finally {
            worker.shutdownNow();
        }
    }
}
