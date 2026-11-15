/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

class SmallHeapEncodingTest {
    @Test
    void largeValuesAreRejectedNormallyInAnIsolatedSmallHeap() throws Exception {
        List<String> modes = new ArrayList<>(Arrays.asList("binary", "extension"));
        try {
            Class.forName("org.eclipse.milo.opcua.stack.core.types.builtin.Matrix");
            modes.add("matrix");
        } catch (ClassNotFoundException legacy) {
            /* Legacy matrices are decoded as Java arrays. */
        }
        for (String mode : modes) {
            Path log = Paths.get("target", "small-heap-" + mode + ".log");
            Process child =
                    new ProcessBuilder(
                                    Paths.get(System.getProperty("java.home"), "bin", "java")
                                            .toString(),
                                    "-Xms32m",
                                    "-Xmx32m",
                                    "-cp",
                                    System.getProperty("java.class.path"),
                                    Probe.class.getName(),
                                    mode)
                            .redirectErrorStream(true)
                            .redirectOutput(log.toFile())
                            .start();
            try {
                assertTrue(
                        child.waitFor(20, TimeUnit.SECONDS), "Small-heap encoder stalled: " + mode);
                String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
                assertEquals(0, child.exitValue(), output);
                assertTrue(output.contains("RecordTooLargeException"), output);
                System.out.print(output);
            } finally {
                if (child.isAlive()) {
                    child.destroyForcibly();
                    assertTrue(child.waitFor(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    void boundedNormalValuesStayWithinTheConfiguredBuffer() throws Exception {
        for (String mode : Arrays.asList("quiet", "workload")) {
            Path log = Paths.get("target", "normal-heap-" + mode + ".log");
            Process child =
                    new ProcessBuilder(
                                    Paths.get(System.getProperty("java.home"), "bin", "java")
                                            .toString(),
                                    "-Xms64m",
                                    "-Xmx256m",
                                    "-XX:+UseSerialGC",
                                    "-XX:-DisableExplicitGC",
                                    "-cp",
                                    System.getProperty("java.class.path"),
                                    NormalValueProbe.class.getName(),
                                    mode)
                            .redirectErrorStream(true)
                            .redirectOutput(log.toFile())
                            .start();
            try {
                assertTrue(
                        child.waitFor(30, TimeUnit.SECONDS), "Normal-value child stalled: " + mode);
                String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
                assertEquals(0, child.exitValue(), output);
                assertTrue(output.contains("NORMAL_HEAP " + mode + " complete"), output);
                System.out.print(output);
            } finally {
                if (child.isAlive()) {
                    child.destroyForcibly();
                    assertTrue(child.waitFor(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    public static class NormalValueProbe {
        private static volatile int consumed;

        public static void main(String[] args) throws Exception {
            boolean workload = args[0].equals("workload");
            OutputSettings output =
                    new OutputSettings("tag,value", "JSON Array", "ISO-8601", false, 0);
            NotificationBuffer buffer = new NotificationBuffer(128, 32768, 256, output);
            for (int i = 0; i < 20; i++) cycle(buffer, output, i, workload);
            System.gc();
            long warmHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            long gcBefore = gcCollections();
            for (int i = 0; i < 200; i++) cycle(buffer, output, i + 20, workload);
            long activeHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            System.gc();
            long finalHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            assertEquals(0, buffer.size());
            assertEquals(0L, buffer.snapshot().get("bytes"));
            assertEquals(false, buffer.snapshot().get("reserved"));
            assertTrue(((Number) buffer.snapshot().get("peakBytes")).longValue() <= 32768);
            System.out.printf(
                    Locale.ROOT,
                    "NORMAL_HEAP %s complete warmupCycles=20 cycles=200 recordsPerCycle=128"
                        + " warmHeap=%d activeHeap=%d finalHeap=%d gcCollections=%d peakBytes=%s"
                        + " consumed=%d%n",
                    args[0],
                    warmHeap,
                    activeHeap,
                    finalHeap,
                    gcCollections() - gcBefore,
                    buffer.snapshot().get("peakBytes"),
                    consumed);
        }

        private static void cycle(
                NotificationBuffer buffer, OutputSettings output, int cycle, boolean workload)
                throws Exception {
            List<byte[]> accepted = new ArrayList<>();
            for (int i = 0; i < 96; i++) {
                byte[] record = normalRecord(cycle, i, output);
                if (workload) {
                    assertTrue(buffer.offer(record));
                    accepted.add(record);
                }
                consumed += record.length;
            }
            List<byte[]> reserved = workload ? buffer.reserve(64) : Collections.emptyList();
            if (workload) assertEquals(accepted.subList(0, 64), reserved);
            for (int i = 96; i < 128; i++) {
                byte[] record = normalRecord(cycle, i, output);
                if (workload) {
                    assertTrue(buffer.offer(record));
                    accepted.add(record);
                }
                consumed += record.length;
            }
            if (workload) {
                assertEquals(128, buffer.size());
                assertEquals(
                        accepted.stream().mapToLong(record -> record.length).sum(),
                        buffer.snapshot().get("bytes"));
                assertFalse(buffer.offer(normalRecord(cycle, 128, output)));
                assertTrue(buffer.reserve(128).isEmpty());
                buffer.acknowledge(reserved.size());
                assertEquals(64, buffer.size());
                assertEquals(accepted.subList(64, 128), buffer.reserve(128));
                buffer.acknowledge(64);
                assertEquals(0, buffer.size());
                assertEquals(0L, buffer.snapshot().get("bytes"));
            }
        }

        private static byte[] normalRecord(int cycle, int index, OutputSettings output)
                throws Exception {
            Object value =
                    index % 3 == 0
                            ? cycle * 128 + index
                            : index % 3 == 1
                                    ? "sample-" + cycle + "-" + index
                                    : new int[] {cycle, index, cycle + index, 7};
            return NotificationJson.encode(
                    MemoryBoundsTest.TAG, new DataValue(new Variant(value)), 256, output);
        }

        private static long gcCollections() {
            long collections = 0;
            for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans())
                if (bean.getCollectionCount() >= 0) collections += bean.getCollectionCount();
            return collections;
        }
    }

    public static class Probe {
        public static void main(String[] args) throws Exception {
            Object raw;
            if (args[0].equals("matrix")) {
                Class<?> matrix =
                        Class.forName("org.eclipse.milo.opcua.stack.core.types.builtin.Matrix");
                raw =
                        matrix.getConstructor(Object.class, int[].class)
                                .newInstance(new int[1000000], new int[] {1000000, 1});
            } else {
                byte[] data = new byte[1536 * 1024];
                Arrays.fill(data, (byte) -1);
                ByteString body = new ByteString(data);
                raw =
                        args[0].equals("binary")
                                ? body
                                : MemoryBoundsTest.extension(body, ByteString.class);
            }
            try {
                NotificationJson.encode(
                        MemoryBoundsTest.TAG, new DataValue(new Variant(raw)), 1024);
                throw new AssertionError("The oversized value must be rejected");
            } catch (IOException failure) {
                for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                    if (cause instanceof NotificationJson.RecordTooLargeException) {
                        System.out.println("SMALL_HEAP " + args[0] + " RecordTooLargeException");
                        return;
                    }
                }
                throw failure;
            }
        }
    }
}
