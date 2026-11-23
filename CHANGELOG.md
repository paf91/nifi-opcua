# Changelog

## 1.0.5-SNAPSHOT — Efficient encoding and configurable ETL output (unreleased)

- Rename the sole processor type to `io.github.paf91.nifi.opcua.ConsumeOPCUA` and remove `SubscribeOpcUa`. Existing processors must be recreated with new IDs and connections using the [migration procedure](docs/namespace-migration.md). NiFi's Change version does not migrate the changed type and bundle artifact.
- Rename the Maven reactor to `nifi-opcua-bundle` and the demo to `nifi-opcua-demo-server`. Use distinct NAR and processor JAR artifact IDs for NiFi 1 and NiFi 2. The `publication` profile prepares standalone consumer POMs and attaches matching sources and Javadoc to both artifacts. Both NARs report `nifi-opcua-nar` as their NiFi runtime bundle ID. Development NAR filenames use `nifi-opcua-<NiFi version>-1.0.5-SNAPSHOT.nar`, and the source archive is `nifi-opcua-source.tar.gz`. The default Application URI `urn:nifi:opcua:subscriber` identifies the OPC UA client. [Maven artifacts](docs/development.md#maven-artifacts)
- Write scalar and primitive-array values directly to the JSON generator; eliminate per-element object-serializer flushes and boxing for primitive numeric arrays. Cache resolved tag NodeId text with size checks.
- Wait at most 50 ms for an in-flight commit instead of applying NiFi's full Yield Duration to queued records. Preserve the single reserved batch and bounded shutdown behavior.
- Roll back synchronous delivery failures before releasing batch and diagnostics reservations, including failures raised as `Error`. Keep submitted commits reserved until their callback settles them, and close connection-owned resources if either adapter constructor fails.
- Hold early notifications within the configured buffer limits until all monitored items succeed. Discard notifications from failed setups. After Stop/Start, wait for retained data and its last commit before connecting with the new buffer.
- Detect a different session ID during session initialization when namespace-URI tags or cached metadata require a new connection. This prevents a transferred subscription's first notification from using an earlier session's tag address or metadata.
- Preserve trailing spaces in string NodeIds in both JSON tag-list forms. Remove unintended padding from JSON string entries when upgrading.
- Use queue count, bytes and adjacent-route transitions for constant-time delayed-batch readiness checks. Preserve FIFO order, route boundaries, framing, age and pressure flushing.
- Declare emitted FlowFile attributes in generated processor help, distinguishing measurements, diagnostics and retained output settings.
- Add Output Fields, Output Format (JSON Array or NDJSON), Timestamp Format (ISO-8601 or Epoch Milliseconds), Route by Quality, and Maximum Batch Delay (ms). Defaults retain existing fields, framing, immediate available batching and the success route.
- Preserve output configuration with buffered records across Stop/Start, retries and stop-time draining. Account for NDJSON newlines in byte limits. Route Bad and Uncertain/missing statuses without inspecting projected JSON or reordering records.
- Add optional serverTimestamp, dataType and engineeringUnits fields. Load requested tag metadata in adaptive batches of up to 100 operations, with one Request Timeout budget, cancellation and bounded cached strings; unavailable metadata remains null. Refresh on a new session; no metadata reads occur per notification.
- Both adapters reduce the batch size when an initial multi-item create request is rejected. NiFi 2 requires a matching service rejection or a local size failure before sending. Both retain previously accepted items.
- Optional metadata reads and browse-path translations reduce the actual rejected batch size, and long EngineeringUnits property identifiers can resolve within OPC UA identifier limits. Metadata failure diagnostics include the symbolic OPC UA status when it is known.
- Correct pre-1970 DateTime conversion, preserving OPC UA 100 ns precision for ISO timestamps and explicitly truncating to milliseconds when requested.
- Add output, timestamp, batching, commit-wait, metadata, allocation and independent-server regressions for both Java profiles. Keep release 1.0.4 artifacts unchanged; this is an unpublished development version.
- Add bounded tag/value sequence checks and periodic fresh-delivery checks to runtime acceptance, with optional per-node sampled heap/thread ceilings. Require profile-specific independent-server reports, including modern setup tests.
- Add JKS and PKCS12 client identity stores and server truststores with explicit format selection, optional independent store/key passwords and deterministic private-key alias selection. Preserve PEM/DER and PKI-directory defaults.
- Keep truststore anchors separate from private-key chains and optional PKI issuer/CRL material. Preserve Full/Certificate revocation checks, read stores without modifying them and reload them for each new processor connection. Clear owned password arrays on identity-load failures as well as success.
- Require final runtime cleanup to observe a stopped processor and zero sessions, subscriptions and monitored items within the polling deadline. Check NiFi 1 cluster ownership by counting fresh queued FlowFiles on each expected node. Record the acceptance script hash and source dirty state alongside its repository commit.
- Prefer supported encrypted username tokens across all endpoints matching the configured channel, including when the first endpoint offers only an unencrypted token. Add Allow Unencrypted Password with None channels only (default), Never and Always (insecure). The Never setting accepts a None-policy token on SignAndEncrypt because the channel encrypts it. Always permits plaintext compatibility without overriding encrypted-token preference or retrying plaintext after certificate validation fails.
- Require migration checks for Sign channels with plaintext username tokens and None channels advertising both encrypted and unencrypted tokens. The former now fail by default; the latter select encryption and require server trust with Full or Certificate validation. See [password-protection migration](docs/configuration.md#password-protection-migration).
- Report an empty provider view of a truststore separately from visible entries without trusted certificates, after successful store loading. Explain PKCS12 trusted-entry markers and preserve password/integrity failures.
- Show the selected trust source and Milo's loaded certificate and CRL counts in NiFi Verify. Flag multiple PEM headers and partial directory checks, and identify the relevant trust anchor and issuer locations on path-building failures. [Certificate troubleshooting](docs/troubleshooting.md#server-certificate-path)
- Manage legacy Guava at 33.8.0-jre. Modern Milo 1.1.8 embeds relocated Guava 33.7.2, which this dependency-management entry does not change.
- Use a shared Maven settings file to mirror only dash-licenses-releases and rso-public-grid to Maven Central in CI and release builds. Bound CI jobs to 60 minutes while retaining enforced NAR documentation generation.
- Return failed futures for modern setup requests cancelled before submission, preserving cleanup requests and atomic cancellation checks.

## 1.0.4 — Bounded stop draining and lower callback allocation (2026-10-22)

- Close ingress first, then attempt to drain buffered notifications on Stop. Add `Stop Drain Timeout (ms)`, default 5000 and configurable from 0 to 30000; zero disables the attempt. Warn only when records remain after draining is unavailable, fails or reaches its deadline.
- Respect batch limits, downstream backpressure and primary ownership during draining. Preserve old source metadata and in-flight reservations across timeouts and Stop/Start. At most one stop-drain worker exists per processor; a blocked repository operation cannot hold the lifecycle wait or accumulate workers across repeated stops.
- Write fixed notification fields directly and format UInt32 status codes without a per-notification map or general formatter. Format diagnostic timestamps only for snapshots. Preserve the 1.0.3 JSON schema and encoded-body migration.
- Add regressions for successful drains, write/commit failures, stalled writes/rollbacks, interruption, backpressure, role changes, byte bounds, late commit outcomes and repeated Stop/Start; verify allocation savings and encoding compatibility on Java 8 and 21.
- Add repository/tooling tests, formatting, dependency advisory checks and the demo build to GitLab CI so the runner covers those GitHub checks before publication.

The drain deadline bounds the processor's additional stop-time wait. NiFi may still wait for an already-active trigger blocked in its repository. Delivery remains in memory and does not recover history or survive a JVM failure before commit.

## 1.0.3 — Bounded encoding and responsive cancellation (2026-10-22)

- Stream opaque ExtensionObject binary/XML/JSON bodies and NiFi 2 matrices through the record byte limit, without constructing full display strings or nested Java arrays. Reject invalid or excessive matrix dimensions.
- Reject new notifications before encoding when local capacity is exhausted, and bound encoding by the remaining byte capacity. Final admission still enforces count and byte limits atomically.
- Keep encoding outside the cancellation monitor; fence the final enqueue so callbacks that finish after Stop or primary revocation cannot add values.
- Separate transport ownership from repository I/O. Stop and primary revocation can close a connection while a content write is blocked; connections completing after cancellation are closed without publication. Pending commit and Stop/Start retention semantics remain unchanged.
- Add allocation, malformed/empty matrix, late-callback, blocked-storage, connection-race and repeated-resource-cleanup regressions for Java 8 and Java 21.
- **Output migration:** ExtensionObject values now contain `encoding`, an `encodingId` (binary/XML) or `typeId` (JSON), and `body`; binary bodies are Base64. Unsupported objects use `encoding: unsupported` and their Java `type`, with no arbitrary display text. Fixed-size UUID/StatusCode display values retain their existing form. See [output](docs/output.md).
- Version both NARs, the demo and CI artifact paths as `1.0.3`. Validation and runtime acceptance evidence are recorded against the exact release artifacts.

## 1.0.2 — Certificate validation and setup fixes (in preparation)

- Add Server Certificate Validation modes Full (default), Certificate (skip hostname/IP only) and None (insecure), for both NiFi adapters and encrypted username tokens. Existing flows retain full validation; None does not require a PKI directory.
- Report the selected validation mode in connection diagnostics and NiFi Verify. Explain validation modes in property help without emitting startup warnings for the selected mode. Client identity, channel protection and user authentication remain independently configured.
- Add shared cryptographic and live-server regression matrices for certificate trust, hostname, URI, validity, usage, signatures and credential failures.
- Bound NiFi 2 initial monitored-item creation and lazy operation-limit reads by Request Timeout, including session acquisition. Each service call gets its own deadline so a healthy large subscription can take longer overall.
- Stop NiFi 2 monitored-item batching on cancellation or timeout and reject late read/create requests after cancellation. Preserve Milo's adaptive batching and automatic recovery error handling after initial setup.
- Preserve the full patch version in release archive/checksum filenames, reject existing output before building, and retain the INCOMPLETE marker if packaging fails.
- Pass the NiFi target explicitly to NAR documentation generation in CI and release builds; this prevents the modern build from loading the legacy API after dependency POMs are reconstructed without active profiles.
- Version the reactor, demo, CI artifact paths and upgrade instructions as `1.0.2`. Java 8/21 tests and packaging checks have passed; full NiFi runtime acceptance is still required before stable publication. See the validation record.

## 1.0.1 — Operation-limit fixes and release versioning (2026-10-21)

- Include the NiFi 1 adaptive monitored-item batching fix and regression tests for missing/stale server limits, cancellation and callback routing.
- Include mandatory independent open62541 integration and recovery tests in GitHub CI, GitLab CI and the release builder.
- Version the reactor, demo and NiFi bundles as `1.0.1`; update CI, image builds and installation commands to the matching artifacts.
- Carry forward Preview 5 connection diagnostics, namespace URI tags, deadbands, byte bounds and configurable security. Delivery remains in memory.
- Existing flows using `1.0.0-SNAPSHOT` can migrate their processor bundle to `1.0.1`; see the installation guide before removing the old NAR.

## Preview 5 — Connection diagnostics and interoperability (2026-10-21)

### Added

- NiFi Verify connection diagnostics and optional health FlowFiles on the `diagnostics` relationship, disabled by default.
- Numeric `ns=` and namespace-URI `nsu=` tag addresses with session-specific resolution and duplicate detection.
- Per-tag Absolute/Percent server deadbands that preserve quality changes and reject unsupported filters.
- Serialized-byte limits for the memory buffer, individual records and success FlowFiles; retained buffers preserve their old limits across configuration changes.
- Configurable OPC TCP security policy/mode and anonymous or username/password authentication on both NiFi targets, including None/None without client certificate files. Existing secure defaults remain unchanged.
- Independent open62541 coverage for security, deadbands, namespace changes, keep-alives, silent-server recovery and unadvertised operation limits. GitHub CI, GitLab CI and the local release builder require these suites to pass without skips.

### Fixed

- NiFi 1 reduces monitored-item request sizes after a service-level `Bad_TooManyOperations`, including absent or stale advertised limits. Accepted chunks are preserved; cancellation, other failures and single-item rejection terminate normally.
- Buffered records preserve their original security policy/mode across stopped configuration changes.
- OpenSSL-exported PEM private keys with leading Bag Attributes are recognized correctly.
- Milo 1 multidimensional values serialize as nested JSON arrays instead of a display-text fallback.
- Subscription failure logs retain the rejected NodeId, OPC UA status and underlying cause, with configured passwords redacted; Milo 1 synchronization failures include item/service details.
- Primary-role notifications use NiFi's current ownership state, so delayed revocation or election events cannot leave a re-elected primary disabled or keep an unowned connection open.
- GitHub CI retains Maven exception logs even when dependency resolution fails before test reports or NARs exist.

### Migration

Existing flows receive the new defaults of 64 MiB buffered JSON, 1 MiB per record and 4 MiB per success FlowFile. Review these limits for large arrays or structures; oversized notifications raise drop counters. Diagnostics reports remain disabled by default. Delivery still uses a bounded in-memory queue; durable recovery across process restarts is deferred.

## Preview 4 — GitHub identity and runtime acceptance (2026-10-19)

- Rename the project and artifacts to `nifi-opcua-subscriber`; align Maven coordinates and Java packages with GitHub owner `paf91`: `io.github.paf91.nifi`. Earlier preview flows require the documented processor-type migration.
- Add disposable full-NiFi runtime, primary-failover and synthetic-load acceptance tooling with hash-linked evidence. Recovery acceptance requires fresh source timestamps for every sampled synthetic value.
- Add a clean-commit local release builder, GitHub Actions builds, NAR verification, dependency inventories and scheduled advisory checks.
- Align Jackson to 2.18.12, Netty to 4.1.139.Final and Bouncy Castle to 1.86 following dependency-advisory review.
- Add Apache-2.0 licensing, third-party notices, English guides, pinned formatting and publication export checks.
- Public examples use synthetic `DemoSwitch` / `ns=2;s=Demo.Boolean` identifiers. Production tag inventories and private development history are excluded from public source snapshots.

Existing demo flows are not overwritten by the seeder. After rebuilding the demo server, update their Tags to `ns=2;s=Demo.Boolean`. Production configurations using their own NodeIds are unaffected.

## Preview 3

- Primary-node connection ownership, cancellation on Stop/revocation and late-callback fencing.
- Explicit bounded discovery and connection-owned transport cleanup.
- Pending data retained across same-JVM Stop/Start and drained with its original endpoint.
- Regression coverage for lifecycle, cancellation, server restart and independent connections.

## Preview 2

- Bulk plaintext/JSON tag lists and certificate-derived Application URI.
- Removed per-record endpoint, schemaVersion and serverTimestamp.
- Demo source disabled by default.

## Preview 1

- Separate legacy and modern NARs, secured subscriptions, JSON output and local NiFi demo.

Preview entries describe historical development milestones built as `1.0.0-SNAPSHOT`. Validation limits remain documented per release.
