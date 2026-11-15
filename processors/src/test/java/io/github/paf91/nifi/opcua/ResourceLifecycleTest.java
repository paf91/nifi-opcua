/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

@Timeout(90)
class ResourceLifecycleTest {
    @TempDir Path dir;

    static long ownedThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(
                        t ->
                                t.getName().startsWith("nifi-opcua-subscriber-transport")
                                        || t.getName().equals("certificate-store-watcher")
                                        || t.getName().equals("milo-trust-list-watcher"))
                .count();
    }

    static long fds() {
        java.lang.management.OperatingSystemMXBean bean =
                ManagementFactory.getOperatingSystemMXBean();
        return bean instanceof com.sun.management.UnixOperatingSystemMXBean
                ? ((com.sun.management.UnixOperatingSystemMXBean) bean).getOpenFileDescriptorCount()
                : -1;
    }

    static void awaitCondition(java.util.function.BooleanSupplier condition, int seconds)
            throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(20);
        assertTrue(condition.getAsBoolean());
    }

    SecureSubscriptionTest server() throws Exception {
        SecureSubscriptionTest fixture = new SecureSubscriptionTest();
        fixture.dir = dir;
        fixture.startServer();
        return fixture;
    }

    static WeakReference<OpcConnection> cycle(OpcSettings settings) throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        OpcConnection connection =
                new OpcConnection(
                        settings, (tag, value) -> received.countDown(), () -> {}, () -> {});
        WeakReference<OpcConnection> ref = new WeakReference<>(connection);
        try {
            assertTrue(received.await(5, TimeUnit.SECONDS));
        } finally {
            connection.close();
        }
        return ref;
    }

    @Test
    void repeatedSecureConnectionsReleaseOwnedResourcesAndKeepOtherClientAlive() throws Exception {
        SecureSubscriptionTest fixture = server();
        try {
            OpcSettings settings =
                    fixture.settings(dir.resolve("pki"), "test-password", "ns=0;i=2258");
            AtomicLong survivorValues = new AtomicLong();
            try (OpcConnection survivor =
                    new OpcConnection(
                            settings,
                            (tag, value) -> survivorValues.incrementAndGet(),
                            () -> {},
                            () -> {})) {
                awaitCondition(() -> survivorValues.get() > 0, 5);
                long baselineThreads = ownedThreads();
                assertTrue(baselineThreads > 0, "The live subscriber must own a transport thread");
                cycle(settings);
                awaitCondition(() -> ownedThreads() <= baselineThreads, 5);
                long baselineFds = fds();
                for (int i = 0; i < 12; i++) cycle(settings);
                long before = survivorValues.get();
                awaitCondition(() -> survivorValues.get() > before, 5);
                awaitResources(baselineThreads, baselineFds, 2);
                System.out.printf(
                        Locale.ROOT,
                        "RESOURCE secure-cycles count=12 ownedThreadsBefore=%d ownedThreadsAfter=%d"
                                + " fdsBefore=%d fdsAfter=%d survivorValues=%d%n",
                        baselineThreads,
                        ownedThreads(),
                        baselineFds,
                        fds(),
                        survivorValues.get());
            }
        } finally {
            fixture.closeServer();
        }
    }

    @Test
    void closedConnectionsCanBeCollectedInAnIsolatedHeap() throws Exception {
        runResourceChild("retention");
    }

    @Test
    void constructorErrorReleasesResourcesWithoutReplacingTheMarker() throws Exception {
        runResourceChild("constructor-error");
    }

    private void runResourceChild(String mode) throws Exception {
        Path log = dir.resolve("resource-" + mode + ".log");
        Path working = Files.createDirectory(dir.resolve(mode));
        Process child =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "java")
                                        .toString(),
                                "-Xms128m",
                                "-Xmx512m",
                                "-XX:+UseSerialGC",
                                "-XX:-DisableExplicitGC",
                                "-cp",
                                System.getProperty("java.class.path"),
                                ResourceProbe.class.getName(),
                                mode,
                                working.toAbsolutePath().toString())
                        .redirectErrorStream(true)
                        .redirectOutput(log.toFile())
                        .start();
        try {
            assertTrue(child.waitFor(70, TimeUnit.SECONDS), "Resource child stalled: " + mode);
            String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
            assertEquals(0, child.exitValue(), output);
            assertTrue(output.contains("RESOURCE_CHILD " + mode + " complete"), output);
            System.out.print(output);
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                assertTrue(child.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }

    private static void awaitResources(long threads, long descriptors, int descriptorAllowance)
            throws Exception {
        awaitCondition(
                () ->
                        ownedThreads() <= threads
                                && (descriptors < 0 || fds() <= descriptors + descriptorAllowance),
                8);
    }

    public static class ResourceProbe {
        public static void main(String[] args) throws Exception {
            ResourceLifecycleTest probe = new ResourceLifecycleTest();
            probe.dir = Paths.get(args[1]);
            if (args[0].equals("retention")) probe.retentionProbe();
            else if (args[0].equals("constructor-error")) probe.constructorErrorProbe();
            else throw new IllegalArgumentException(args[0]);
            System.out.println("RESOURCE_CHILD " + args[0] + " complete");
        }
    }

    private void retentionProbe() throws Exception {
        SecureSubscriptionTest fixture = server();
        try {
            OpcSettings settings =
                    fixture.settings(dir.resolve("pki"), "test-password", "ns=0;i=2258");
            AtomicLong survivorValues = new AtomicLong();
            try (OpcConnection survivor =
                    new OpcConnection(
                            settings,
                            (tag, value) -> survivorValues.incrementAndGet(),
                            () -> {},
                            () -> {})) {
                awaitCondition(() -> survivorValues.get() > 0, 5);
                List<WeakReference<OpcConnection>> references = new ArrayList<>();
                for (int i = 0; i < 12; i++) references.add(cycle(settings));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (references.stream().anyMatch(reference -> reference.get() != null)
                        && System.nanoTime() < deadline) {
                    System.gc();
                    Thread.sleep(100);
                }
                long reachable =
                        references.stream().filter(reference -> reference.get() != null).count();
                long before = survivorValues.get();
                awaitCondition(() -> survivorValues.get() > before, 5);
                assertEquals(0, reachable, "Closed connections retained after controlled GC");
                System.out.println(
                        "RESOURCE retention closed=12 reachable="
                                + reachable
                                + " survivorValues="
                                + survivorValues.get());
            }
        } finally {
            fixture.closeServer();
        }
    }

    private void constructorErrorProbe() throws Exception {
        SecureSubscriptionTest fixture = server();
        try {
            OpcSettings settings =
                    fixture.settings(dir.resolve("pki"), "test-password", "ns=0;i=2258");
            AtomicLong survivorValues = new AtomicLong();
            try (OpcConnection survivor =
                    new OpcConnection(
                            settings,
                            (tag, value) -> survivorValues.incrementAndGet(),
                            () -> {},
                            () -> {})) {
                awaitCondition(() -> survivorValues.get() > 0, 5);
                long baselineThreads = ownedThreads();
                cycle(settings);
                awaitCondition(() -> ownedThreads() <= baselineThreads, 5);
                long baselineFds = fds();
                AssertionError marker = new AssertionError("endpoint description marker");
                AtomicInteger discovered = new AtomicInteger();
                AtomicLong threadsAtDiscovery = new AtomicLong();
                ConnectionDiagnostics diagnostics =
                        new ConnectionDiagnostics(
                                value -> {
                                    if (value.startsWith("opc.tcp://")) {
                                        discovered.incrementAndGet();
                                        threadsAtDiscovery.set(ownedThreads());
                                        throw marker;
                                    }
                                    return value;
                                });
                ConnectionAttempt attempt = new ConnectionAttempt();
                AssertionError actual =
                        assertThrows(
                                AssertionError.class,
                                () ->
                                        new OpcConnection(
                                                settings,
                                                (tag, value) -> {},
                                                () -> {},
                                                () -> {},
                                                attempt,
                                                diagnostics));
                assertSame(marker, actual);
                assertEquals(
                        1, discovered.get(), "Injection must follow a real endpoint discovery");
                assertEquals("Discovering endpoints", diagnostics.snapshot().get("stage"));
                assertTrue(
                        threadsAtDiscovery.get() > baselineThreads,
                        "Discovery must have used the constructor's transport");
                assertTrue(attempt.isCancelled());
                awaitResources(baselineThreads, baselineFds, 2);
                long before = survivorValues.get();
                awaitCondition(() -> survivorValues.get() > before, 5);
                cycle(settings);
                awaitResources(baselineThreads, baselineFds, 2);
                System.out.printf(
                        Locale.ROOT,
                        "RESOURCE constructor-error discovered=%d markerPreserved=true"
                                + " ownedThreadsBefore=%d ownedThreadsAfter=%d fdsBefore=%d"
                                + " fdsAfter=%d%n",
                        discovered.get(),
                        baselineThreads,
                        ownedThreads(),
                        baselineFds,
                        fds());
            }
        } finally {
            fixture.closeServer();
        }
    }

    @Test
    void aSlowLiveValueCallbackDoesNotBlockStopOrLeakALateValue() throws Exception {
        SecureSubscriptionTest fixture = server();
        CountDownLatch callback = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<ConnectionAttempt> generation = new AtomicReference<>();
        AtomicReference<NotificationBuffer> destination = new AtomicReference<>();
        ConsumeOPCUA processor =
                new ConsumeOPCUA() {
                    @Override
                    protected AutoCloseable openConnection(
                            ProcessContext context, ConnectionAttempt attempt) throws Exception {
                        generation.set(attempt);
                        return super.openConnection(context, attempt);
                    }

                    @Override
                    void accept(
                            NotificationBuffer buffer,
                            TagDefinition tag,
                            DataValue value,
                            ConnectionAttempt attempt) {
                        destination.set(buffer);
                        callback.countDown();
                        try {
                            assertTrue(release.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        super.accept(buffer, tag, value, attempt);
                    }
                };
        TestRunner runner = ProcessorTest.configured(processor);
        runner.setProperty(ConsumeOPCUA.ENDPOINT, fixture.endpoint);
        runner.setProperty(ConsumeOPCUA.CERTIFICATE, dir.resolve("client.der").toString());
        runner.setProperty(ConsumeOPCUA.PRIVATE_KEY, dir.resolve("client.pem").toString());
        runner.setProperty(ConsumeOPCUA.PKI, dir.resolve("pki").toString());
        runner.setProperty(ConsumeOPCUA.PASSWORD, "test-password");
        runner.setProperty("DemoSwitch", "ns=0;i=2258");
        processor.start(runner.getProcessContext());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        Future<?> trigger =
                workers.submit(
                        () ->
                                processor.onTrigger(
                                        runner.getProcessContext(),
                                        runner.getProcessSessionFactory().createSession()));
        Future<?> stop = null;
        try {
            assertTrue(callback.await(5, TimeUnit.SECONDS));
            stop = workers.submit(processor::unscheduled);
            stop.get(4, TimeUnit.SECONDS);
            assertTrue(generation.get().isCancelled());
            assertEquals(0, destination.get().size());
        } finally {
            release.countDown();
            trigger.get(5, TimeUnit.SECONDS);
            if (stop != null) stop.get(5, TimeUnit.SECONDS);
            processor.stop();
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(3, TimeUnit.SECONDS));
            fixture.closeServer();
        }
        assertEquals(0, destination.get().size(), "Late callback must not enqueue after Stop");
    }

    @Test
    void repeatedSilentHandshakesCloseSocketsAndTransportThreads() throws Exception {
        try (SilentEndpoint warmup = new SilentEndpoint()) {
            OpcSettings settings =
                    new OpcSettings(
                            warmup.url(),
                            null,
                            null,
                            null,
                            "",
                            null,
                            null,
                            null,
                            100,
                            150,
                            Collections.singletonList(
                                    TagDefinition.parse("time", "ns=0;i=2258", 100, 10)),
                            new OpcSecurity("None", "None", "Anonymous"));
            assertThrows(
                    Exception.class,
                    () -> new OpcConnection(settings, (tag, value) -> {}, () -> {}, () -> {}));
            assertTrue(warmup.disconnected.await(2, TimeUnit.SECONDS));
        }
        long baseline = ownedThreads(), baselineFds = fds();
        for (int i = 0; i < 12; i++) {
            try (SilentEndpoint silent = new SilentEndpoint()) {
                OpcSettings settings =
                        new OpcSettings(
                                silent.url(),
                                null,
                                null,
                                null,
                                "",
                                null,
                                null,
                                null,
                                100,
                                150,
                                Collections.singletonList(
                                        TagDefinition.parse("time", "ns=0;i=2258", 100, 10)),
                                new OpcSecurity("None", "None", "Anonymous"));
                assertThrows(
                        Exception.class,
                        () -> new OpcConnection(settings, (tag, value) -> {}, () -> {}, () -> {}));
                assertTrue(silent.accepted.await(2, TimeUnit.SECONDS));
                assertTrue(silent.disconnected.await(2, TimeUnit.SECONDS));
            }
        }
        awaitResources(baseline, baselineFds, 4);
        System.out.printf(
                Locale.ROOT,
                "RESOURCE silent-handshakes count=12 ownedThreadsBefore=%d ownedThreadsAfter=%d"
                        + " fdsBefore=%d fdsAfter=%d%n",
                baseline,
                ownedThreads(),
                baselineFds,
                fds());
    }
}
