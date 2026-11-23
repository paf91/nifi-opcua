/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.apache.nifi.processor.Relationship;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Peek before commit: callbacks can append, but cannot evict an uncommitted batch. */
final class NotificationBuffer {
    private final int capacity;
    private final long byteCapacity;
    private final int maximumRecordBytes;
    private long bytes;
    private long peakBytes;
    private int relationshipTransitions;
    private boolean reserved;
    final OutputSettings output;
    private ArrayDeque<Entry> queue = new ArrayDeque<>();

    private static final class Entry {
        final byte[] value;
        final Relationship relationship;
        final long received;

        Entry(byte[] value, Relationship relationship) {
            this.value = value;
            this.relationship = relationship;
            received = System.nanoTime();
        }
    }

    NotificationBuffer(int capacity) {
        this(capacity, Long.MAX_VALUE, Integer.MAX_VALUE);
    }

    NotificationBuffer(int capacity, long byteCapacity, int maximumRecordBytes) {
        this(capacity, byteCapacity, maximumRecordBytes, OutputSettings.DEFAULT);
    }

    NotificationBuffer(
            int capacity, long byteCapacity, int maximumRecordBytes, OutputSettings output) {
        if (capacity < 1
                || byteCapacity < 1
                || maximumRecordBytes < 1
                || maximumRecordBytes > byteCapacity)
            throw new IllegalArgumentException(
                    "Buffer and record limits must be positive; record limit must fit buffer");
        this.capacity = capacity;
        this.byteCapacity = byteCapacity;
        this.maximumRecordBytes = maximumRecordBytes;
        this.output = Objects.requireNonNull(output);
    }

    synchronized NotificationBuffer emptyCopy() {
        NotificationBuffer copy =
                new NotificationBuffer(capacity, byteCapacity, maximumRecordBytes, output);
        copy.peakBytes = peakBytes;
        return copy;
    }

    /** Release staged records even if a cancelled callback still references this buffer. */
    synchronized void discard() {
        queue = new ArrayDeque<>();
        bytes = 0;
        relationshipTransitions = 0;
        reserved = false;
        notifyAll();
    }

    synchronized boolean offer(byte[] value) {
        return offer(value, ConsumeOPCUA.SUCCESS);
    }

    synchronized boolean offer(byte[] value, Relationship relationship) {
        Objects.requireNonNull(value);
        if (value.length > maximumRecordBytes
                || queue.size() >= capacity
                || value.length > byteCapacity - bytes) return false;
        Entry previous = queue.peekLast();
        queue.addLast(new Entry(value, Objects.requireNonNull(relationship)));
        if (previous != null && !previous.relationship.equals(relationship))
            relationshipTransitions++;
        bytes += value.length;
        peakBytes = Math.max(peakBytes, bytes);
        notifyAll();
        return true;
    }

    /** A cheap admission snapshot; offer still enforces both limits atomically. */
    synchronized int encodingLimit() {
        if (queue.size() >= capacity) return 0;
        return (int) Math.min(maximumRecordBytes, byteCapacity - bytes);
    }

    synchronized List<byte[]> peek(int maximum) {
        List<byte[]> batch = new ArrayList<>();
        for (Entry item : queue) {
            if (batch.size() == maximum) break;
            batch.add(item.value);
        }
        return batch;
    }

    synchronized List<byte[]> reserve(int maximum) {
        return reserve(maximum, Long.MAX_VALUE);
    }

    synchronized List<byte[]> reserve(int maximum, long maximumBytes) {
        if (reserved) return Collections.emptyList();
        List<byte[]> batch = new ArrayList<>();
        long encoded = output.ndjson ? 0 : 2;
        Relationship relationship = nextRelationship();
        for (Entry item : queue) {
            long addition = item.value.length + (output.ndjson || !batch.isEmpty() ? 1L : 0L);
            if (batch.size() == maximum
                    || addition > maximumBytes - encoded
                    || !item.relationship.equals(relationship)) break;
            batch.add(item.value);
            encoded += addition;
        }
        reserved = !batch.isEmpty();
        return batch;
    }

    synchronized Relationship nextRelationship() {
        return queue.isEmpty() ? ConsumeOPCUA.SUCCESS : queue.getFirst().relationship;
    }

    /** Wait on notifications/commit completion, never poll at a zero scheduling period. */
    synchronized boolean awaitBatch(int maximum, long maximumBytes, long deadline)
            throws InterruptedException {
        while (!queue.isEmpty()
                && !reserved
                && !batchReady(maximum, maximumBytes, System.nanoTime())) {
            long remaining =
                    Math.min(
                            deadline - System.nanoTime(),
                            output.batchDelayNanos
                                    - (System.nanoTime() - queue.getFirst().received));
            if (remaining <= 0) break;
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        return !reserved && batchReady(maximum, maximumBytes, System.nanoTime());
    }

    synchronized boolean batchReady(int maximum, long maximumBytes, long now) {
        if (queue.isEmpty() || reserved) return false;
        if (output.batchDelayNanos == 0
                || now - queue.getFirst().received >= output.batchDelayNanos
                || queue.size() >= capacity
                || byteCapacity - bytes < maximumRecordBytes) return true;
        int count = queue.size();
        long framingBytes = output.ndjson ? count : count + 1L;
        return relationshipTransitions > 0
                || count >= maximum
                || bytes >= maximumBytes - framingBytes;
    }

    synchronized void release() {
        reserved = false;
        notifyAll();
    }

    synchronized void acknowledge(int count) {
        if (count < 0 || count > queue.size())
            throw new IllegalArgumentException("Invalid acknowledgement count");
        for (int i = 0; i < count; i++) {
            Entry removed = queue.removeFirst();
            bytes -= removed.value.length;
            if (!queue.isEmpty() && !removed.relationship.equals(queue.getFirst().relationship))
                relationshipTransitions--;
        }
        reserved = false;
        notifyAll();
    }

    /** Wait only for the previous batch's outcome; never steal an in-flight reservation. */
    synchronized void awaitAvailable(long deadline) throws InterruptedException, TimeoutException {
        while (reserved) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException();
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
    }

    synchronized int size() {
        return queue.size();
    }

    int maximumRecordBytes() {
        return maximumRecordBytes;
    }

    synchronized Map<String, Object> snapshot() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("records", queue.size());
        state.put("recordCapacity", capacity);
        state.put("bytes", bytes);
        state.put("byteCapacity", byteCapacity);
        state.put("peakBytes", peakBytes);
        state.put("reserved", reserved);
        return state;
    }
}
