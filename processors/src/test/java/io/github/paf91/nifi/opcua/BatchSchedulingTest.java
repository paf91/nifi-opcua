/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;

import org.apache.nifi.processor.*;
import org.apache.nifi.util.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

@Timeout(15)
class BatchSchedulingTest {
    private static final TagDefinition TAG = MemoryBoundsTest.TAG;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void pendingCommitWaitsWithoutRequestingYieldAndRetainsDataOnFailure(boolean success)
            throws Exception {
        ConsumeOPCUA p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.BATCH, "1");
        r.setProperty(ConsumeOPCUA.STOP_DRAIN_TIMEOUT, "0");
        p.start(r.getProcessContext());
        p.acceptForTest(TAG, new DataValue(new Variant(1)));
        p.acceptForTest(TAG, new DataValue(new Variant(2)));
        ProcessSession first = spy(r.getProcessSessionFactory().createSession());
        AtomicReference<Runnable> complete = new AtomicReference<>();
        AtomicReference<Consumer<Throwable>> failed = new AtomicReference<>();
        doAnswer(
                        call -> {
                            complete.set(call.getArgument(0));
                            failed.set(call.getArgument(1));
                            return null;
                        })
                .when(first)
                .commitAsync(any(Runnable.class), any());
        p.onTrigger(r.getProcessContext(), first);
        ProcessContext context = spy(r.getProcessContext());
        ProcessSession blocked = spy(r.getProcessSessionFactory().createSession());
        long started = System.nanoTime();
        p.onTrigger(context, blocked);
        long elapsed = System.nanoTime() - started;
        assertTrue(
                elapsed >= TimeUnit.MILLISECONDS.toNanos(35),
                "Must not busy-spin while a commit is pending");
        assertTrue(elapsed < TimeUnit.SECONDS.toNanos(2), "Wait must be bounded");
        verify(context, never()).yield();
        verify(blocked, never()).create();
        assertEquals(2, records(p, r));
        if (success) {
            first.commitAsync();
            complete.get().run();
        } else {
            first.rollback();
            failed.get().accept(new IllegalStateException("injected failure"));
        }
        r.run(2, false, false);
        assertEquals(Arrays.asList(1, 2), OutputOptionsTest.values(r, ConsumeOPCUA.SUCCESS));
        assertEquals(0, records(p, r));
        p.stop();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void commitCallbackLetsTheWaitingTriggerDrainWithoutAnotherInvocation(boolean success)
            throws Exception {
        ConsumeOPCUA processor = new ProcessorTest.OfflineProcessor();
        TestRunner runner = ProcessorTest.configured(processor);
        runner.setProperty(ConsumeOPCUA.BATCH, "1");
        runner.setProperty(ConsumeOPCUA.STOP_DRAIN_TIMEOUT, "0");
        processor.start(runner.getProcessContext());
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            processor.acceptForTest(TAG, new DataValue(new Variant(1)));
            processor.acceptForTest(TAG, new DataValue(new Variant(2)));
            ProcessSession first = spy(runner.getProcessSessionFactory().createSession());
            AtomicReference<Runnable> committed = new AtomicReference<>();
            AtomicReference<Consumer<Throwable>> failed = new AtomicReference<>();
            doAnswer(
                            call -> {
                                committed.set(call.getArgument(0));
                                failed.set(call.getArgument(1));
                                return null;
                            })
                    .when(first)
                    .commitAsync(any(Runnable.class), any());
            processor.onTrigger(runner.getProcessContext(), first);
            assertNotNull(committed.get());
            assertNotNull(failed.get());
            java.lang.reflect.Field field = ConsumeOPCUA.class.getDeclaredField("buffer");
            field.setAccessible(true);
            NotificationBuffer buffer = (NotificationBuffer) field.get(processor);
            ProcessContext context = spy(runner.getProcessContext());
            boolean callbackDriven = false;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            for (int attempt = 0;
                    attempt < 20 && !callbackDriven && System.nanoTime() < deadline;
                    attempt++) {
                AtomicReference<Thread> waitingThread = new AtomicReference<>();
                ProcessSession waitingSession =
                        spy(runner.getProcessSessionFactory().createSession());
                Future<?> invocation;
                synchronized (buffer) {
                    invocation =
                            worker.submit(
                                    () -> {
                                        waitingThread.set(Thread.currentThread());
                                        processor.onTrigger(
                                                context,
                                                (ProcessSessionFactory) () -> waitingSession);
                                    });
                    long rendezvousDeadline =
                            Math.min(deadline, System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
                    while (!invocation.isDone() && System.nanoTime() < rendezvousDeadline) {
                        buffer.wait(2);
                        Thread thread = waitingThread.get();
                        boolean inAwaitAvailable = false;
                        if (thread != null && thread.getState() == Thread.State.TIMED_WAITING) {
                            for (StackTraceElement frame : thread.getStackTrace()) {
                                if (frame.getClassName().equals(NotificationBuffer.class.getName())
                                        && frame.getMethodName().equals("awaitAvailable")) {
                                    inAwaitAvailable = true;
                                    break;
                                }
                            }
                        }
                        if (inAwaitAvailable && thread.getState() == Thread.State.TIMED_WAITING) {
                            verify(waitingSession, never()).create();
                            assertEquals(2, buffer.size());
                            assertEquals(true, buffer.snapshot().get("reserved"));
                            if (success) {
                                first.commitAsync();
                                committed.get().run();
                            } else {
                                first.rollback();
                                failed.get()
                                        .accept(
                                                new IllegalStateException(
                                                        "injected commit callback failure"));
                            }
                            callbackDriven = true;
                            break;
                        }
                    }
                }
                invocation.get(2, TimeUnit.SECONDS);
                if (callbackDriven) {
                    verify(waitingSession).create();
                    verify(waitingSession).commitAsync(any(Runnable.class), any());
                } else {
                    verify(waitingSession, never()).create();
                    runner.assertTransferCount(ConsumeOPCUA.SUCCESS, 0);
                    assertEquals(2, buffer.size());
                    assertEquals(true, buffer.snapshot().get("reserved"));
                }
            }
            assertTrue(callbackDriven, "No invocation entered the pending-commit wait");
            verify(context, never()).yield();
            assertEquals(
                    success ? Arrays.asList(1, 2) : Collections.singletonList(1),
                    OutputOptionsTest.values(runner, ConsumeOPCUA.SUCCESS));
            assertEquals(success ? 0 : 1, buffer.size());
            assertEquals(false, buffer.snapshot().get("reserved"));
        } finally {
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(3, TimeUnit.SECONDS));
            processor.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void reservationSettlementWakesAwaitAvailable(boolean acknowledge) throws Exception {
        NotificationBuffer buffer = new NotificationBuffer(4);
        assertTrue(buffer.offer(new byte[] {1}));
        assertTrue(buffer.offer(new byte[] {2}));
        assertEquals(1, buffer.reserve(1).size());
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> waiting =
                    worker.submit(
                            () -> {
                                waitingThread.set(Thread.currentThread());
                                buffer.awaitAvailable(
                                        System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
                                return null;
                            });
            awaitTimedWait(waitingThread, "awaitAvailable");
            assertFalse(waiting.isDone());
            if (acknowledge) buffer.acknowledge(1);
            else buffer.release();
            waiting.get(3, TimeUnit.SECONDS);
            assertEquals(acknowledge ? 1 : 2, buffer.size());
            assertEquals(false, buffer.snapshot().get("reserved"));
        } finally {
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void anOfferWakesAwaitBatchWhenTheCountBoundaryIsReached() throws Exception {
        OutputSettings output =
                OutputOptionsTest.output("tag,value", "JSON Array", "ISO-8601", false, 60000);
        NotificationBuffer buffer = new NotificationBuffer(10, 1000, 20, output);
        assertTrue(buffer.offer(new byte[] {1}));
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> waiting =
                    worker.submit(
                            () -> {
                                waitingThread.set(Thread.currentThread());
                                return buffer.awaitBatch(
                                        2, 100, System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
                            });
            awaitTimedWait(waitingThread, "awaitBatch");
            assertFalse(waiting.isDone());
            assertTrue(buffer.offer(new byte[] {2}));
            assertTrue(waiting.get(3, TimeUnit.SECONDS));
        } finally {
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    private static void awaitTimedWait(AtomicReference<Thread> reference, String method)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            Thread thread = reference.get();
            if (thread != null && thread.getState() == Thread.State.TIMED_WAITING) {
                for (StackTraceElement frame : thread.getStackTrace())
                    if (frame.getClassName().equals(NotificationBuffer.class.getName())
                            && frame.getMethodName().equals(method)) return;
            }
            Thread.sleep(10);
        }
        fail("Worker did not enter NotificationBuffer." + method + " timed wait");
    }

    @ParameterizedTest
    @ValueSource(strings = {"JSON Array", "NDJSON"})
    void readinessMatchesScanningReferenceAcrossRoutesAndPartialAcknowledgements(String format) {
        OutputSettings output =
                OutputOptionsTest.output("tag,value", format, "ISO-8601", true, 60000);
        NotificationBuffer buffer = new NotificationBuffer(19, 100, 16, output);
        List<ReferenceEntry> expected = new ArrayList<>();
        Random random = new Random(192837L);
        boolean reserved = false;
        int reservedCount = 0;
        long peak = 0;
        for (int step = 0; step < 6000; step++) {
            switch (random.nextInt(5)) {
                case 0:
                case 1:
                    byte[] value = new byte[1 + random.nextInt(16)];
                    Arrays.fill(value, (byte) step);
                    Relationship route =
                            random.nextInt(3) == 1 ? ConsumeOPCUA.BAD : ConsumeOPCUA.SUCCESS;
                    long bytes = expected.stream().mapToLong(e -> e.value.length).sum();
                    boolean fits = expected.size() < 19 && bytes + value.length <= 100;
                    assertEquals(fits, buffer.offer(value, route));
                    if (fits) expected.add(new ReferenceEntry(value, route, System.nanoTime()));
                    peak = Math.max(peak, bytes + (fits ? value.length : 0));
                    break;
                case 2:
                    int count = 1 + random.nextInt(23);
                    long maximumBytes = 1 + random.nextInt(130);
                    List<byte[]> actual = buffer.reserve(count, maximumBytes);
                    List<byte[]> wanted =
                            reserved
                                    ? Collections.emptyList()
                                    : referenceBatch(expected, count, maximumBytes, output.ndjson);
                    assertEquals(wanted, actual, "reservation at step " + step);
                    if (!reserved) {
                        reservedCount = wanted.size();
                        reserved = reservedCount > 0;
                    }
                    break;
                case 3:
                    int acknowledged = reservedCount == 0 ? 0 : random.nextInt(reservedCount + 1);
                    buffer.acknowledge(acknowledged);
                    expected.subList(0, acknowledged).clear();
                    reserved = false;
                    reservedCount = 0;
                    break;
                default:
                    buffer.release();
                    reserved = false;
                    reservedCount = 0;
            }
            assertEquals(expected.size(), buffer.size());
            assertEquals(referenceValues(expected), buffer.peek(100));
            assertEquals(
                    expected.stream().mapToLong(e -> e.value.length).sum(),
                    buffer.snapshot().get("bytes"));
            assertEquals(peak, buffer.snapshot().get("peakBytes"));
            assertEquals(reserved, buffer.snapshot().get("reserved"));
            assertEquals(
                    expected.isEmpty() ? ConsumeOPCUA.SUCCESS : expected.get(0).route,
                    buffer.nextRelationship());
            for (int maximum : new int[] {1, 3, 19, 23}) {
                for (long maximumBytes : new long[] {1, 2, 17, 33, 100, 130}) {
                    long now = System.nanoTime();
                    assertEquals(
                            referenceReady(expected, reserved, maximum, maximumBytes, now, output),
                            buffer.batchReady(maximum, maximumBytes, now),
                            "readiness at step " + step);
                    now += TimeUnit.MINUTES.toNanos(2);
                    assertEquals(
                            !reserved && !expected.isEmpty(),
                            buffer.batchReady(maximum, maximumBytes, now));
                }
            }
        }
        buffer.release();
        buffer.acknowledge(buffer.size());
        assertFalse(buffer.batchReady(1, 100, System.nanoTime()));
        assertTrue(buffer.offer(new byte[] {7}, ConsumeOPCUA.BAD));
        assertFalse(buffer.batchReady(2, 100, System.nanoTime()));
        assertEquals(ConsumeOPCUA.BAD, buffer.nextRelationship());
    }

    @ParameterizedTest
    @ValueSource(strings = {"JSON Array", "NDJSON"})
    void mixedRoutesAndPartialHeadAcknowledgementsKeepExactBoundaries(String format) {
        OutputSettings output =
                OutputOptionsTest.output("tag,value", format, "ISO-8601", true, 60000);
        NotificationBuffer buffer = new NotificationBuffer(10, 100, 10, output);
        byte[] a = {1, 1}, b = {2, 2, 2}, c = {3}, d = {4, 4};
        buffer.offer(a);
        buffer.offer(b);
        buffer.offer(c, ConsumeOPCUA.BAD);
        buffer.offer(d);
        assertTrue(buffer.batchReady(10, 100, System.nanoTime()));
        assertEquals(Arrays.asList(a, b), buffer.reserve(10, 100));
        buffer.acknowledge(1);
        assertTrue(buffer.batchReady(10, 100, System.nanoTime()));
        assertEquals(Collections.singletonList(b), buffer.reserve(10, 100));
        buffer.acknowledge(1);
        assertEquals(Collections.singletonList(c), buffer.reserve(10, 100));
        buffer.acknowledge(1);
        long exact = output.ndjson ? 3 : 4;
        assertFalse(buffer.batchReady(2, exact + 1, System.nanoTime()));
        assertTrue(buffer.batchReady(2, exact, System.nanoTime()));
        assertEquals(Collections.singletonList(d), buffer.reserve(2, exact));
        buffer.acknowledge(1);
        assertEquals(0L, buffer.snapshot().get("bytes"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JSON Array", "NDJSON"})
    void largeGrowingPartialBatchesRemainUnreadyUntilARealBoundary(String format) {
        OutputSettings output =
                OutputOptionsTest.output("tag,value", format, "ISO-8601", false, 60000);
        NotificationBuffer buffer = new NotificationBuffer(40000, 1000000, 16, output);
        for (int i = 0; i < 20000; i++) {
            assertTrue(buffer.offer(new byte[] {1, 2, 3}));
            assertFalse(buffer.batchReady(30000, 900000, System.nanoTime()));
        }
        assertTrue(buffer.batchReady(20000, 900000, System.nanoTime()));
        assertEquals(20000, buffer.reserve(30000, 900000).size());
        buffer.acknowledge(19999);
        assertFalse(buffer.batchReady(30000, 900000, System.nanoTime()));
        assertTrue(
                buffer.batchReady(30000, 900000, System.nanoTime() + TimeUnit.MINUTES.toNanos(2)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void producerAppendsWhileReservedWithoutEvictingAcceptedRecords(boolean success)
            throws Exception {
        NotificationBuffer buffer = new NotificationBuffer(4, 10, 4);
        byte[] a = {1, 1}, b = {2, 2, 2}, c = {3, 3}, d = {4, 4, 4};
        assertTrue(buffer.offer(a));
        assertTrue(buffer.offer(b));
        List<byte[]> first = buffer.reserve(2);
        assertEquals(Arrays.asList(a, b), first);
        CountDownLatch append = new CountDownLatch(1), appended = new CountDownLatch(1);
        ExecutorService producer = Executors.newSingleThreadExecutor();
        try {
            Future<?> result =
                    producer.submit(
                            () -> {
                                assertTrue(append.await(3, TimeUnit.SECONDS));
                                assertTrue(buffer.offer(c));
                                assertTrue(buffer.offer(d));
                                assertFalse(buffer.offer(new byte[] {5}));
                                appended.countDown();
                                return null;
                            });
            assertEquals(5L, buffer.snapshot().get("bytes"));
            append.countDown();
            assertTrue(appended.await(3, TimeUnit.SECONDS));
            result.get(3, TimeUnit.SECONDS);
            assertEquals(10L, buffer.snapshot().get("bytes"));
            assertEquals(10L, buffer.snapshot().get("peakBytes"));
            assertEquals(Arrays.asList(a, b, c, d), buffer.peek(10));
            assertTrue(buffer.reserve(4).isEmpty());
            List<byte[]> delivered = new ArrayList<>();
            if (success) {
                delivered.addAll(first);
                buffer.acknowledge(first.size());
                assertEquals(5L, buffer.snapshot().get("bytes"));
            } else buffer.release();
            List<byte[]> remainder = buffer.reserve(4);
            delivered.addAll(remainder);
            buffer.acknowledge(remainder.size());
            assertEquals(Arrays.asList(a, b, c, d), delivered);
            assertEquals(0, buffer.size());
            assertEquals(0L, buffer.snapshot().get("bytes"));
            assertEquals(false, buffer.snapshot().get("reserved"));
        } finally {
            append.countDown();
            producer.shutdownNow();
            assertTrue(producer.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    private static final class ReferenceEntry {
        final byte[] value;
        final Relationship route;
        final long received;

        ReferenceEntry(byte[] value, Relationship route, long received) {
            this.value = value;
            this.route = route;
            this.received = received;
        }
    }

    private static List<byte[]> referenceValues(List<ReferenceEntry> entries) {
        List<byte[]> values = new ArrayList<>();
        for (ReferenceEntry entry : entries) values.add(entry.value);
        return values;
    }

    private static List<byte[]> referenceBatch(
            List<ReferenceEntry> entries, int maximum, long maximumBytes, boolean ndjson) {
        List<byte[]> result = new ArrayList<>();
        long encoded = ndjson ? 0 : 2;
        for (ReferenceEntry entry : entries) {
            long addition = entry.value.length + (ndjson || !result.isEmpty() ? 1L : 0L);
            if (result.size() == maximum
                    || addition > maximumBytes - encoded
                    || !entry.route.equals(entries.get(0).route)) break;
            result.add(entry.value);
            encoded += addition;
        }
        return result;
    }

    private static boolean referenceReady(
            List<ReferenceEntry> entries,
            boolean reserved,
            int maximum,
            long maximumBytes,
            long now,
            OutputSettings output) {
        if (entries.isEmpty() || reserved) return false;
        long bytes = entries.stream().mapToLong(e -> e.value.length).sum();
        if (now - entries.get(0).received >= output.batchDelayNanos
                || entries.size() >= 19
                || 100 - bytes < 16) return true;
        int count = 0;
        long encoded = output.ndjson ? 0 : 2;
        for (ReferenceEntry entry : entries) {
            long addition = entry.value.length + (output.ndjson || count > 0 ? 1L : 0L);
            if (!entry.route.equals(entries.get(0).route) || addition > maximumBytes - encoded)
                return true;
            encoded += addition;
            if (++count >= maximum || encoded == maximumBytes) return true;
        }
        return false;
    }

    @Test
    void batchDelayFlushesOnAgeCountBytesRouteChangeAndPressure() {
        OutputSettings settings =
                OutputOptionsTest.output("tag,value", "JSON Array", "ISO-8601", true, 1000);
        NotificationBuffer b = new NotificationBuffer(10, 100, 30, settings);
        b.offer(new byte[10]);
        long now = System.nanoTime();
        assertFalse(b.batchReady(3, 50, now));
        assertTrue(b.batchReady(3, 50, now + TimeUnit.SECONDS.toNanos(1)));
        b.offer(new byte[10]);
        assertTrue(b.batchReady(2, 50, now));
        assertTrue(b.batchReady(3, 20, now));
        b.offer(new byte[10], ConsumeOPCUA.BAD);
        assertTrue(b.batchReady(10, 100, now));
        assertEquals(2, b.reserve(10, 100).size());
        b.acknowledge(2);
        assertEquals(ConsumeOPCUA.BAD, b.nextRelationship());
        assertFalse(b.batchReady(10, 100, now));
        assertTrue(
                b.batchReady(10, 100, now + TimeUnit.SECONDS.toNanos(2)),
                "Acknowledgement must not reset arrival age");
        NotificationBuffer countPressure = new NotificationBuffer(1, 100, 30, settings);
        countPressure.offer(new byte[10]);
        assertTrue(countPressure.batchReady(10, 100, System.nanoTime()));
        NotificationBuffer bytePressure = new NotificationBuffer(10, 50, 30, settings);
        bytePressure.offer(new byte[30]);
        assertTrue(bytePressure.batchReady(10, 100, System.nanoTime()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JSON Array", "NDJSON"})
    void framingRespectsExactByteLimits(String format) {
        OutputSettings settings =
                OutputOptionsTest.output("tag,value", format, "ISO-8601", false, 1000);
        NotificationBuffer b = new NotificationBuffer(10, 100, 10, settings);
        b.offer(new byte[10]);
        b.offer(new byte[10]);
        long size = format.equals("NDJSON") ? 22 : 23;
        assertTrue(b.batchReady(10, size, System.nanoTime()));
        assertEquals(1, b.reserve(10, size - 1).size());
        b.release();
        assertEquals(2, b.reserve(10, size).size());
    }

    @Test
    void sparseBatchWaitIsBoundedAndStopDrainsWithoutWaitingForBatchAge() throws Exception {
        ConsumeOPCUA p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.BATCH_DELAY, "60000");
        r.setProperty(ConsumeOPCUA.OUTPUT_FORMAT, "NDJSON");
        r.setProperty(ConsumeOPCUA.OUTPUT_FIELDS, "tag,value");
        p.start(r.getProcessContext());
        p.acceptForTest(TAG, new DataValue(new Variant(7)));
        ProcessContext context = spy(r.getProcessContext());
        long started = System.nanoTime();
        p.onTrigger(context, r.getProcessSessionFactory());
        assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
        verify(context, never()).yield();
        r.assertTransferCount(ConsumeOPCUA.SUCCESS, 0);
        assertEquals(1, records(p, r));
        p.stop();
        assertEquals(0, records(p, r));
        r.assertTransferCount(ConsumeOPCUA.SUCCESS, 1);
        MockFlowFile flow = r.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS).get(0);
        flow.assertAttributeEquals("mime.type", "application/x-ndjson");
        assertEquals(
                7,
                new ObjectMapper().readTree(r.getContentAsByteArray(flow)).get("value").intValue());
        assertTrue(r.getLogger().getWarnMessages().isEmpty());
    }

    @Test
    void retainedRecordsKeepFormatProjectionTimestampAndRouteAfterSettingsChange()
            throws Exception {
        ConsumeOPCUA p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.STOP_DRAIN_TIMEOUT, "0");
        r.setProperty(ConsumeOPCUA.OUTPUT_FORMAT, "NDJSON");
        r.setProperty(ConsumeOPCUA.OUTPUT_FIELDS, "tag,value,sourceTimestamp");
        r.setProperty(ConsumeOPCUA.TIMESTAMP_FORMAT, "Epoch Milliseconds");
        r.setProperty(ConsumeOPCUA.QUALITY_ROUTING, "true");
        p.start(r.getProcessContext());
        p.acceptForTest(TAG, new DataValue(new Variant(17), StatusCode.BAD, DateTime.now(), null));
        p.stop();
        r.setProperty(ConsumeOPCUA.OUTPUT_FORMAT, "JSON Array");
        r.setProperty(ConsumeOPCUA.OUTPUT_FIELDS, OutputSettings.DEFAULT_FIELDS);
        r.setProperty(ConsumeOPCUA.TIMESTAMP_FORMAT, "ISO-8601");
        r.setProperty(ConsumeOPCUA.QUALITY_ROUTING, "false");
        r.setProperty(ConsumeOPCUA.BATCH_DELAY, "60000");
        assertTrue(p.getRelationships().contains(ConsumeOPCUA.BAD));
        p.start(r.getProcessContext());
        r.run(1, false, false);
        r.assertTransferCount(ConsumeOPCUA.BAD, 1);
        MockFlowFile old = r.getFlowFilesForRelationship(ConsumeOPCUA.BAD).get(0);
        old.assertAttributeEquals("opcua.output.format", "NDJSON");
        old.assertAttributeEquals("opcua.output.fields", "tag,value,sourceTimestamp");
        old.assertAttributeEquals("opcua.timestamp.format", "Epoch Milliseconds");
        JsonNode json = new ObjectMapper().readTree(r.getContentAsByteArray(old));
        assertEquals(3, json.size());
        assertTrue(json.get("sourceTimestamp").isNumber());
        r.setProperty(ConsumeOPCUA.BATCH_DELAY, "0");
        r.run(1, false, false); // install the new buffer and subscription
        p.acceptForTest(TAG, new DataValue(new Variant(18), StatusCode.BAD, DateTime.now(), null));
        r.run(1, false, false);
        assertEquals(
                Collections.singletonList(18), OutputOptionsTest.values(r, ConsumeOPCUA.SUCCESS));
        r.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS)
                .get(0)
                .assertAttributeEquals("opcua.output.format", "JSON Array");
        p.stop();
    }

    @Test
    void qualityBackpressureRetainsTheHeadAndStopDoesNotBypassIt() throws Exception {
        ConsumeOPCUA p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.QUALITY_ROUTING, "true");
        p.start(r.getProcessContext());
        p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
        p.acceptForTest(TAG, new DataValue(new Variant(1), StatusCode.BAD, null, null));
        p.acceptForTest(TAG, new DataValue(new Variant(2), StatusCode.GOOD, null, null));
        r.setRelationshipUnavailable(ConsumeOPCUA.BAD);
        p.onTrigger(r.getProcessContext(), r.getProcessSessionFactory());
        p.stop();
        assertEquals(2, records(p, r));
        r.assertTransferCount(ConsumeOPCUA.BAD, 0);
        r.assertTransferCount(ConsumeOPCUA.SUCCESS, 0);
        r.setRelationshipAvailable(ConsumeOPCUA.BAD);
        p.start(r.getProcessContext());
        r.run(2, false, false);
        assertEquals(Collections.singletonList(1), OutputOptionsTest.values(r, ConsumeOPCUA.BAD));
        assertEquals(
                Collections.singletonList(2), OutputOptionsTest.values(r, ConsumeOPCUA.SUCCESS));
        p.stop();
    }

    private static int records(ConsumeOPCUA p, TestRunner r) {
        return (Integer)
                ((Map<?, ?>) p.diagnosticSnapshot(r.getProcessContext()).get("buffer"))
                        .get("records");
    }
}
