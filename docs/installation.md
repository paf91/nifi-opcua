# Installation, upgrade and rollback

The current development branch builds **1.0.5-SNAPSHOT** with processor type `io.github.paf91.nifi.opcua.ConsumeOPCUA` and bundle `io.github.paf91.nifi:nifi-opcua-nar:1.0.5-SNAPSHOT`. It provides no `SubscribeOpcUa` compatibility type. Existing installations need the [processor migration](namespace-migration.md) before replacing their old NAR.

Release **1.0.4** retains its original `SubscribeOpcUa` type and artifact names. Its installation and upgrade instructions remain below.

[Configuration reference](configuration.md) · [Troubleshooting](troubleshooting.md)

## Pick the artifact

Development build:

| Target | Artifact |
|---|---|
| NiFi 1.23.2 / Java 8 | `nifi-opcua-1.23.2-1.0.5-SNAPSHOT.nar` |
| NiFi 2.12.0 / Java 21 | `nifi-opcua-2.12.0-1.0.5-SNAPSHOT.nar` |

Released version 1.0.4:

| Target | Artifact |
|---|---|
| NiFi 1.23.2 / Java 8 | `nifi-opcua-subscriber-1.23.2-1.0.4.nar` |
| NiFi 2.12.0 / Java 21 | `nifi-opcua-subscriber-2.12.0-1.0.4.nar` |

Build from source or obtain a tested CI/release artifact. Check the SHA-256 against the accompanying checksum manifest. A filename containing SNAPSHOT is not a unique release identity: keep the source commit and checksum with every deployment. Do not unpack the NAR or install individual dependency JARs.

## First installation using autoload

If `conf/nifi.properties` already contains, for example:

```properties
nifi.nar.library.directory=/opt/nifi/lib
nifi.nar.library.autoload.directory=/opt/nifi/extensions
nifi.nar.working.directory=/opt/nifi/work/nar/
```

copy the complete NAR into `/opt/nifi/extensions`. Stage it outside the watched directory, then move the completed file into place (use the same filesystem for an atomic rename). NiFi's service account needs read access and directory traversal permission.

Wait for NiFi to load the extension, refresh the browser, and search for **ConsumeOPCUA** in Add Processor for the development build, or **SubscribeOpcUa** for release 1.0.4. First-time autoload does not require restart. Do not put files into `work/nar`: it is managed by NiFi. Keep only one copy of each bundle's coordinates across all configured NAR library and autoload directories.

## Installation using a custom library

Create an administrator-owned directory, for example `/opt/nifi/custom-nars`, and add:

```properties
nifi.nar.library.directory.opcua=/opt/nifi/custom-nars
```

Keep the existing `nifi.nar.library.directory` property. During a maintenance window, stop NiFi, place the NAR in the custom directory, and restart using the same service manager/account used for the installation. Relative paths are resolved against NiFi home.

These mechanisms are documented in the [NiFi 1.23.2 administration guide](https://github.com/apache/nifi/blob/rel/nifi-1.23.2/nifi-docs/src/main/asciidoc/administration-guide.adoc#processor-locations).

## Before starting a subscription

- Supply the production endpoint, complete NodeIds, and separate production credentials.
- Provision client certificate/key and server trust material on the NiFi host or container.
- Grant the operator the restricted-component permissions for filesystem read/write.
- Connect `success` to a downstream processor; keep the source stopped until ready.
- Verify initial values, types, timestamps, quality and the absence of unexpected bulletins.
- Set downstream backpressure and monitor drops. A RUNNING state alone does not prove an OPC session is established.

## Upgrade

The move from `SubscribeOpcUa` to `ConsumeOPCUA` changes both the stored processor type and the bundle artifact. Follow the [processor migration](namespace-migration.md), which recreates processors and connections. **Change version** does not perform this migration, even when the old and new builds both use version `1.0.5-SNAPSHOT`.

For an installed `ConsumeOPCUA`, use **Change version** only for a target with the same processor type, Maven group and artifact. Stop and drain in-memory notifications before changing versions, or explicitly accept their loss. Back up the flow and installed NAR, deploy the target to every cluster node, check properties and relationships, run Verify, and confirm fresh data after starting.

When upgrading an earlier development build, review the [password-protection migration checks](configuration.md#password-protection-migration) before Verify or Start. Sign channels with plaintext username tokens now fail by default. None channels offering both encrypted and unencrypted tokens select encryption and require server trust with Full or Certificate validation.

### Upgrading the earlier processor to release 1.0.4

These steps apply to the earlier GitHub-aligned `SubscribeOpcUa` type and `nifi-opcua-subscriber-nar` artifact:

1. Record the installed NAR checksum, processor properties and flow configuration using your normal secure backup procedure.
2. Stop the source and drain pending data according to your ingestion policy. NiFi restart and processor version changes discard this processor's in-memory pending records.
3. Install release 1.0.4 alongside the previous version under its distinct filename. Install the matching NiFi target on every cluster node. Restart NiFi when using a custom library directory; a new bundle version can also use autoload.
4. On the stopped `SubscribeOpcUa`, use **Change version** and select **1.0.4**. Confirm bundle group `io.github.paf91.nifi`, artifact `nifi-opcua-subscriber-nar` and version `1.0.4`.
5. Check properties, relationships and validation, run Verify, then start and confirm fresh data. Once no components reference the previous bundle, remove its NAR during a maintenance restart.

**Server Certificate Validation** defaults to **Full**, preserving existing checks when upgrading from 1.0.1 or earlier. Review any deliberate change to Certificate or None before starting. See NiFi's [component version instructions](https://nifi.apache.org/nifi-docs/user-guide.html#changing-component-versions). Earlier private `ai.paf91.nifi` types need the processor migration instead.

## Replacing a build with the same bundle coordinates

After migrating or installing the new artifact, use this procedure to replace one `io.github.paf91.nifi:nifi-opcua-nar:1.0.5-SNAPSHOT` build with another. **Change version** cannot distinguish builds with identical group, artifact and version. The earlier `nifi-opcua-subscriber-nar:1.0.5-SNAPSHOT` has a different artifact and requires the processor migration above.

1. Record the installed NAR's SHA-256 and source commit, and back up the flow configuration and properties securely.
2. Stop the source. Confirm that its memory buffer drained successfully, resolve any remaining records, or explicitly decide to discard them before restarting NiFi.
3. Stop NiFi on every cluster node. Move the installed NAR outside every configured NAR library and autoload directory, and retain it as a backup.
4. Install exactly one copy of the replacement for those bundle coordinates across all scanned directories on each node. Use the same NAR bytes on every cluster node and retain the replacement's checksum and source commit.
5. Restart NiFi, check that the `ConsumeOPCUA` processors resolve, run Verify, then start and confirm fresh data.

Do not delete repositories or NiFi's working directories as a routine upgrade step. A restart loses any notifications still held in processor memory.

## Rollback

A rollback from `ConsumeOPCUA` to the earlier type requires the previous NAR and a compatible flow backup. First stop the new source and successfully drain its memory and downstream queues, or explicitly decide to discard their remaining data. Follow the [migration rollback procedure](namespace-migration.md#rollback). **Change version** does not restore the old processor type or artifact.

For a rollback between builds with identical `nifi-opcua-nar` coordinates, use the maintenance replacement procedure above with the previous NAR. NiFi repositories and flow backups must remain consistent. Do not roll them back independently or delete caches and repositories to resolve a missing processor type. A NAR rollback does not restore notifications lost from memory.

## Cluster requirements

Deploy the same NAR bytes and certificate/trust paths on all eligible nodes. Install the NAR containing `ConsumeOPCUA` on every cluster node before adding that type to the flow. The source is restricted to the primary node. A new primary creates a fresh subscription; the old node's memory queue is not replicated. Confirm certificate identity and trust requirements with the OPC server administrator when provisioning multiple clients. Complete the [cluster acceptance checks](operations.md#cluster-acceptance) before production failover use.
