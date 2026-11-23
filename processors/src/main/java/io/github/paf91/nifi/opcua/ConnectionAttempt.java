/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import java.util.concurrent.*;

/** One connection generation; cancellation also fences late callbacks. */
final class ConnectionAttempt {
    final NotificationBuffer destination;
    private boolean staged;
    private volatile boolean cancelled;
    private Thread blockingThread;

    ConnectionAttempt() {
        this(null);
    }

    ConnectionAttempt(NotificationBuffer destination) {
        this.destination = destination;
        staged = destination != null;
    }

    synchronized void publish(Runnable publication) {
        check();
        publication.run();
        staged = false;
    }

    synchronized void cancel() {
        cancelled = true;
        if (blockingThread != null) blockingThread.interrupt();
        if (staged) {
            destination.discard();
            staged = false;
        }
    }

    boolean isCancelled() {
        return cancelled;
    }

    void check() {
        if (cancelled) throw new CancellationException("OPC UA connection attempt cancelled");
    }

    /** Fence short state changes only; never encode values or perform blocking I/O here. */
    synchronized void ifActive(Runnable action) {
        if (!cancelled) action.run();
    }

    /** Submit a nonblocking request atomically with respect to cancellation. */
    synchronized <T> T callIfActive(java.util.function.Supplier<T> action) {
        check();
        return action.get();
    }

    <T> T await(CompletableFuture<T> future, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        try {
            while (true) {
                check();
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new TimeoutException("OPC UA operation timed out");
                try {
                    return future.get(
                            Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50)),
                            TimeUnit.NANOSECONDS);
                } catch (TimeoutException e) {
                    if (System.nanoTime() >= deadline) throw e;
                }
            }
        } catch (Exception e) {
            future.cancel(false);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw e;
        }
    }

    void blocking(Runnable action) {
        synchronized (this) {
            check();
            blockingThread = Thread.currentThread();
        }
        try {
            action.run();
            check();
        } finally {
            synchronized (this) {
                blockingThread = null;
                // Do not leak our cancellation interrupt into NiFi's reusable worker.
                if (cancelled) Thread.interrupted();
            }
        }
    }
}
