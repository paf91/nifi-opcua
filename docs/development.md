# Development and release preparation

## Local checks

Run from the repository root. Use JDK 8 for the legacy release build and JDK 21 for the modern release build. Also verify the modern profile under JDK 25:

```sh
python3 scripts/check-project.py
python3 -m unittest discover -s tests -v

mvn -B -ntp clean verify
python3 scripts/verify-nar.py nar/target/nifi-opcua-1.23.2-1.0.5-SNAPSHOT.nar 52
python3 scripts/nar-inventory.py nar/target/nifi-opcua-1.23.2-1.0.5-SNAPSHOT.nar --output nar/target/dependency-inventory.json
```

Save the legacy NAR/reports outside target, switch to JDK 21, then:

```sh
mvn -B -ntp -Pnifi2 -Dnifi.version=2.12.0 clean verify
python3 scripts/verify-nar.py nar/target/nifi-opcua-2.12.0-1.0.5-SNAPSHOT.nar 65
python3 scripts/nar-inventory.py nar/target/nifi-opcua-2.12.0-1.0.5-SNAPSHOT.nar --output nar/target/dependency-inventory.json
python3 scripts/format-java.py --check
```

Save the modern NAR/reports outside target, switch to JDK 25, and repeat the modern Maven build, NAR verification and inventory commands above. The modern profile still compiles for Java 21, with class-file version 65. Java 25 provides additional build/test verification. There remain two NAR variants, and the release builder uses JDK 8 and JDK 21. A successful Java 25 build and packaging check does not establish a deployed NiFi runtime on Java 25 or compatibility with the current Apache NiFi source reactor.

The explicit `-Dnifi.version=2.12.0` is required for the modern NAR build. The NAR plugin reconstructs dependency POMs without the command-line profile selection when choosing its documentation API. Supplying the version as a system property keeps documentation generation on the same NiFi API used for compilation. CI and the release builder pass this property automatically.

CI and the release builder pass `-s scripts/maven-ci-settings.xml` to every Maven invocation, including the demo build. That settings file mirrors only the `dash-licenses-releases` and `rso-public-grid` repository IDs to `https://repo.maven.apache.org/maven2`. Local builds can add the same option, for example `mvn -s scripts/maven-ci-settings.xml -B -ntp clean verify`. The option selects the user settings file for that invocation. If you need existing user settings such as credentials or other mirrors, merge this narrow mirror into those settings and select the merged file. No global Maven configuration change is needed.

To apply formatting, omit `--check`. The formatter downloads a pinned JAR from Maven Central and verifies its committed SHA-256 before execution; it is stored in ignored `.tools`. Formatting uses JDK 21 even when editing shared Java 8 sources.

Integration tests cover the 22 supported policy/mode/authentication combinations, plus encrypted username tokens over a None channel with trusted, missing and untrusted PKI. They generate temporary keys/certificates and use ephemeral loopback ports. They require neither production credentials nor the Docker demo. CI additionally builds an independent open62541 fixture and sets `OPCUA_OPEN62541_BINARY` to enable its security, deadband, verification and namespace-restart tests. Local runs skip `Open62541Test`, `Open62541RecoveryTest` and the modern `Open62541SetupTest` unless that variable points to the compiled fixture. Rebuild the fixture after changing `tests/interop/open62541.c`; an old cached executable does not test the changed fixture source. The optional `ExternalServerTest` is skipped unless `OPCUA_TEST_ENDPOINT` is set; see [server interoperability](interoperability.md). Python utility tests use only the standard library, including rejection of stale synthetic source timestamps after faults. Build the demo separately with JDK 8 using `mvn -f demo/pom.xml clean package`.

After a fixture-enabled build, check the required reports for that profile:

```sh
python3 scripts/verify-interop-tests.py processors/target/surefire-reports --profile nifi1
```

Use `--profile nifi2` for the modern build. Both profiles require `Open62541Test` and `Open62541RecoveryTest`; the modern profile also requires `Open62541SetupTest`. Missing, empty, skipped or failed required suites fail the check.

## CI

GitHub Actions runs repository checks and three build/test combinations on pull requests and pushes: legacy on Java 8, modern on Java 21 and modern on Java 25. Both modern builds target Java 21 bytecode. It uses read-only permissions, pinned official actions and Maven 3.9.11 Docker images. Artifacts remain `nifi1-artifacts` and `nifi2-artifacts` for the release targets, with `nifi2-java25-artifacts` for the extra verification build. Maven runs with full exception output, and its console logs are saved under `dist/ci-logs/` and uploaded even if dependency resolution fails before tests start. Test reports are also uploaded when available; a downloadable artifact does not itself imply the job passed. CI does not publish a release or deploy a flow.

The GitLab pipeline builds the same checksum-pinned open62541 fixture in a separate Ubuntu job and supplies it to all three Maven jobs. Its Java 25 job inherits the modern build configuration. Both CI providers limit jobs to 60 minutes, check the JUnit reports and fail if an independent-server suite is missing or skipped. All three builds retain dependency inventory and advisory checks. GitLab also runs repository checks, Python tests, Java formatting and the Java 8 demo build, matching those GitHub checks before publication. Keep the successful GitLab pipeline link with the release evidence; local successful tests do not imply that remote CI ran.

CI and the local release builder cap the Maven heap at 512 MiB and the Surefire test JVM heap at 1 GiB. Explicit small-heap child-process settings in resource tests remain in effect.

## Artifact verification

The NAR verifier checks processor registration, generated extension documentation, bundled class-file versions and exclusion of provided/test-only dependencies. Multi-release JVM-specific classes are excluded from the base-bytecode test. This is packaging verification, not exhaustive dependency compatibility or vulnerability analysis.

The processor help includes `additionalDetails.html` and native `additionalDetails.md` under the `ConsumeOPCUA` documentation directory. Keep their technical content aligned so both NiFi targets receive the full help.

The dependency inventory describes the actual bundled JARs, including their hashes and embedded license/notice paths. An artifact can contain multiple Maven coordinate records because of shading; source-JAR URLs are candidates that must be checked before redistribution. Keep the inventory and checksums with each binary release.

Root dependency management selects `com.google.guava:guava:33.8.0-jre` for the legacy build, including the default and explicit `nifi1` profiles. Modern Milo 1.1.8 embeds relocated Guava 33.7.2. The managed dependency does not replace that embedded copy, so inspect each actual NAR inventory when reviewing dependencies.

## Dependency advisory check

After saving each target's dependency inventory, query the public package versions:

```sh
python3 scripts/check-advisories.py path/to/nifi1-inventory.json path/to/nifi2-inventory.json --output advisory-report.json
```

This requires network access and sends only public Maven coordinates/versions to OSV. An advisory match or an unidentified JAR causes a nonzero exit; network or incomplete-response errors also fail rather than reporting a clean result. Review the report's excluded/unversioned components. Zero version matches does not cover the NiFi installation, JDK, containers, unreported issues or exploitability. Advisory results change over time.

## Publishing source without private history

Use a clean, committed tree:

```sh
python3 scripts/check-project.py --history
python3 scripts/export-public.py
```

The exporter creates `dist/nifi-opcua-source.tar.gz` with root directory `nifi-opcua/` and adjacent JSON metadata containing the originating commit and SHA-256. It exports tracked source only: no ignored state, binary build output, private documents or Git history. Extract it into a new directory and inspect it before initializing a new Git repository.

**Do not mirror private development history into a public repository.** Deleting a file in a later commit does not remove earlier copies. The first public import should use the clean snapshot when the private history contains customer material. Do not copy the private repository's `.git` directory, refs or tags.

For a reviewed snapshot, the publication sequence is:

1. Extract and inspect the source archive.
2. Run repository checks and both builds from that clean tree.
3. Initialize a new repository and commit the reviewed snapshot.
4. Create the GitHub repository under the intended owner and add its remote.
5. Push only the new public history.
6. Enable private vulnerability reporting and branch protection; require CI checks.
7. Verify that GitHub Actions actually passes before attaching a binary release.

The GitHub repository is `paf91/nifi-opcua`. Updating local source or artifacts does not publish them. The preparation scripts do not create repositories or push changes.

## Automated local release

After committing the intended source, run:

```sh
python3 scripts/release.py --output dist/release-1.0.5-SNAPSHOT --maven-cache ~/.m2
```

The output directory must not exist. This builds from a clean Git archive in a temporary directory, not from existing Maven target directories. It builds the pinned open62541 fixture, requires its integration and recovery suites in both profiles, verifies NAR packaging and formatting, builds the demo, queries OSV, and creates a bundle containing source, binaries, test reports, independent-server logs, dependency inventories, commit/image metadata and SHA-256 checksums. Both builds use the same commit-derived archive timestamp by default. For a reproducibility check, pass `--output-timestamp` with the previous bundle’s `outputTimestamp` and compare NAR hashes; source and tool changes can still change the result. This improves artifact traceability; bit-for-bit reproducibility across arbitrary toolchains is not claimed.

The archive and its checksum retain the full output name: `dist/release-1.0.5-SNAPSHOT.tar.gz` and `dist/release-1.0.5-SNAPSHOT.tar.gz.sha256`. The archive's internal root is `nifi-opcua-release`. Existing archives or checksum files are rejected before building and are never overwritten. Failed output retains an `INCOMPLETE` marker, including packaging failures, and must not be distributed. The script never pushes, publishes or deploys. Runtime acceptance is separate: run [the acceptance harness](acceptance.md) against the resulting NARs and retain its hash-linked reports. Do not label a new binary runtime-tested merely because an earlier binary passed.

The CI workflow also checks bundled dependency advisories on pushes, pull requests, manual runs and a weekly schedule. An API failure fails the check; review the uploaded report rather than treating unavailable advisory data as a pass.

## Practical change review

Review the supported deployment configurations as well as individual code paths:

- Cover PEM/DER, JKS and PKCS12 identities, password-protected and empty-password stores, PKCS12 stores without a MAC, independent key passwords, aliases and server trust sources. Check failure messages and secret redaction as well as successful loading.
- Exercise Stop/Start, connection replacement, Verify, primary ownership, retained records, pending commits and resource cleanup. Include certificate/trust rotation with the same loading rules used by real connections.
- Preserve meaningful test oracles: verify delivered values and metadata, rejected configurations, callback ownership and cleanup outcomes. Similar final values do not make tests of different lifecycle paths redundant.
- Check CPU/allocation changes with representative ordinary values and a reference behavior. Distinguish direct primitive-array inputs from boxed arrays returned by the SDK's binary decoder. Preserve null-element and numeric encoding semantics. State the measured scope rather than inferring CPU savings, throughput or long-term leak freedom from allocation results.
- Build and inspect both NAR variants, generated help and FlowFile attributes. Check migration instructions against the actual processor type, bundle coordinates and existing flows.
- Bind local tests, remote CI and real NiFi acceptance to their exact source and artifact hashes. Missing runtime evidence remains missing even when unit and integration suites pass.

## Release checklist

- Record source commit, NAR checksums, exact NiFi/JDK targets and test results.
- Inspect the source export for proprietary data and credentials; the pattern scanner is only a limited check.
- Run both build profiles, packaging checks, formatter and utility tests.
- Review dependency advisories and third-party licenses/source availability for the resolved artifacts.
- Perform a real NiFi smoke test for each supported runtime; disclose missing coverage.
- Perform full two-node failover and representative-load tests before claiming production cluster readiness.
- Update the changelog and migration notes; choose a release version deliberately.
- For a non-SNAPSHOT release, update reactor/demo versions and artifact paths in CI/docs together, then rebuild from that exact commit.
- Publish both NAR variants, checksums, inventories, test evidence and a corresponding source snapshot.
- Label pre-release artifacts as such until acceptance criteria are met.

No benchmark, supported-version expansion, long-running leak test, current vulnerability clearance or full-cluster certification is implied by a passing unit/integration build.

For known artifacts without embedded Maven metadata (including Bouncy Castle), the inventory records filename-derived coordinates separately as `knownFilenameCoordinates`. The advisory checker includes them; unknown JARs remain explicitly listed as exclusions.
