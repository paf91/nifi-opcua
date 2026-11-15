# Runtime, cluster and load acceptance

The acceptance harness runs disposable Docker containers with generated test identities and synthetic `Load.Tag0` through `Load.Tag999` nodes. It creates its own network, uses random loopback HTTP ports, and removes its containers and network when finished. It does not modify the interactive preview.

Requirements: Docker, Python 3.9+, two unused Docker subnets, and approximately 8 GB of free memory for concurrent runs. The acceptance HTTP configuration has no login and is only for these isolated local containers. It is not a production deployment template. Content archival is disabled in these disposable containers so unrelated host disk usage does not throttle the test repository.

Each container has a two-CPU limit. Memory limits are 3 GiB per NiFi node, 1 GiB for the demo server and 512 MiB for ZooKeeper.

## Prepare

Build a release using [the release script](development.md#automated-local-release). Build the demo server with Java 8:

```sh
mvn -B -ntp -f demo/pom.xml clean package
docker build -f acceptance/Dockerfile.nifi1 -t opcua-acceptance-nifi1:local .
docker pull apache/nifi:2.12.0
docker pull zookeeper:3.9.4
```

## Run

Use the NARs from one release directory. Each output directory must be new:

```sh
python3 scripts/acceptance.py --profile nifi1 \
  --nar dist/release-1.0.5-SNAPSHOT/nifi1/nifi-opcua-1.23.2-1.0.5-SNAPSHOT.nar \
  --output dist/acceptance/legacy --subnet 172.30.248.0/24 --tags 1000 --seconds 1800

python3 scripts/acceptance.py --profile nifi2 \
  --nar dist/release-1.0.5-SNAPSHOT/nifi2/nifi-opcua-2.12.0-1.0.5-SNAPSHOT.nar \
  --output dist/acceptance/modern --subnet 172.30.249.0/24 --tags 1000 --seconds 1800
```

NiFi 1 runs two nodes with ZooKeeper; NiFi 2 runs one node. Both use secure OPC UA subscriptions. `--seconds` is the steady-load duration after the recovery scenarios, not total runtime. Zero skips the soak. Increase it to 86400 for a 24-hour soak. A 30-minute run does not establish long-term leak freedom.

The harness checks NAR discovery and validation, sampled JSON quality, active subscription and monitored-item counts, backpressure recovery, Stop/Start cleanup, server restart, a stalled server process, and delivery recovery. Final cleanup uses a 30-second polling deadline and requires NiFi to report the source as STOPPED with zero active tasks and the server to report zero sessions, subscriptions and monitored items in one metrics observation. The final-stop-cleanup check records that successful observation. On the legacy cluster it stops the primary, checks election and delivery on the survivor, restarts the old primary, and checks single-subscription ownership after rejoin.

The harness sets `DEMO_LOAD_VALUE_MODE=indexed`: each synthetic value is `tick * 10000 + tagIndex`, including initial values. The demo's default `tick` mode gives every tag the current tick. The report records the selected mode and stride so the content check can distinguish tag/value mix-ups.

Before intentional faults, a full run temporarily selects at most 16 tags, requests 100 ms sampling/publishing intervals and 32-entry monitored-item queues, and captures queued output in queue-position order. The capture must finish within 30 seconds, contain fewer than 80 FlowFiles and 2048 records, and keep each content response below 4 MiB. Reaching a cap or receiving a truncated queue listing fails the check. Every tick strictly between the recorded start/end bounds must contain each selected tag, with at least two such ticks. The harness also checks exact tag alias, NodeId and value associations plus strictly increasing ticks for each tag. It restores the full-load configuration before pressure and recovery tests. This bounded phase does not assert lossless delivery during intentional faults.

Use `--etl-only` for a shorter output/configuration check instead of recovery and load scenarios. It validates default delivery, switches to projected NDJSON with epoch timestamps and cached metadata, connects the additional quality relationships, then restores the default JSON output. The demo provides Good values and no engineering-unit property, so this runtime check verifies the null metadata fallback; Java tests with open62541 cover actual engineering units and tests with controlled statuses cover Bad/Uncertain routing. This mode omits the controlled sequence phase, soak, restart recovery and failover.

During the soak, the harness periodically checks a newly delivered FlowFile and fresh source/receipt timestamps as well as subscription count and per-node JVM resources. Optional `--max-heap-mib` and `--max-threads` accept positive finite ceilings applied separately to every expected NiFi node during this sampled soak. Heap uses `usedHeapBytes`, with 1 MiB = 1048576 bytes. Threads use JVM `totalThreads`. A configured ceiling fails if any expected node or required metric is missing or invalid, or a sample exceeds the limit. Select limits for the test environment. Passing these sampled checks does not establish leak freedom or bound whole-process memory.

For these once-per-second synthetic tags, every sampled record must have a source timestamp and receipt timestamp less than 30 seconds old, with no timestamp more than 5 seconds in the future. Recent receipt of an old source value is rejected. This freshness rule is specific to the synthetic acceptance server; production subscription values may legitimately remain unchanged for longer periods.

The simulated network stall pauses the server container; it is not an asymmetric cluster network partition. Graceful primary shutdown is not a simultaneous coordinator/ZooKeeper failure. These scenarios require additional deployment testing.

## Evidence and cleanup

`report.json` records the exact NAR SHA-256, the source repository's `harnessCommit` and `sourceTreeDirty` (resolved from the script location), and the executing script's `harnessSha256`. If Git metadata is unavailable or the source directory belongs to an ancestor checkout, the commit and dirty state are null and `harnessProvenanceError` explains the problem. The script hash is still recorded.

The report also contains checks, timestamps, per-node JVM heap/thread samples, NiFi status/counters and server subscription metrics. It retains the latest 256 raw samples. Resource-budget maxima and sample counts accumulate during the soak. Resource budgets record configured limits, per-node observed maxima/sample counts and per-metric `passed`, `failed`, `not_configured` or `not_run` status. With `--seconds 0` or `--etl-only`, resource budgets remain `not_run`.

A passing run requires all assertions to complete. Failed runs retain an error and `passed: false`. Runtime logs and generated keys remain in the ignored output directory. Share only the sanitized JSON report after review. Do not publish generated keys or the security directory.

The host directory must be writable by the container UID used for the demo and readable by NiFi's UID 1000. The Linux development environment uses UID 1000. Adjust permissions deliberately for other hosts.

Subprocess commands have fixed timeouts. On normal exit or an ordinary exception the harness attempts to remove every container, its anonymous volumes and the network independently, even if log collection fails. Cleanup errors are recorded and fail the run. If the process is forcibly killed, identify leftovers by the `opcua.acceptance` Docker label and remove only that run's containers/network. Avoid blanket Docker cleanup commands.

For container-wide memory (including more than the Java heap), CPU and PID samples, run the optional collector alongside an active acceptance test. Obtain the exact names with `docker ps --filter label=opcua.acceptance --format '{{.Names}}'`, then pass only the desired NiFi containers:

```sh
python3 scripts/collect-container-stats.py --output dist/acceptance/container-resources.json \
  --containers RUN-node1 RUN-node2 --seconds 2400
```

The collector checks the acceptance label, records Docker cgroup accounting, and finishes when the selected containers stop or the duration expires. It does not alter containers. Native memory attribution and multi-day leak analysis require additional profiling.

Use `--demo-target` to point to a separate compiled demo directory when another test is using the default `demo/target`. Do not rebuild a directory mounted by a running test server.
