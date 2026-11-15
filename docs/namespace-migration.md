# Migrating to ConsumeOPCUA

The current development build registers only `io.github.paf91.nifi.opcua.ConsumeOPCUA`. It does not register the earlier `SubscribeOpcUa` type. Existing processors must be recreated.

| Identity | Earlier public build | Current development build |
|---|---|---|
| Maven group | `io.github.paf91.nifi` | `io.github.paf91.nifi` |
| NAR artifact | `nifi-opcua-subscriber-nar` | `nifi-opcua-nar` |
| Processor type | `io.github.paf91.nifi.opcua.SubscribeOpcUa` | `io.github.paf91.nifi.opcua.ConsumeOPCUA` |

Release **1.0.4** keeps its original artifact names and processor type. Earlier development builds and the renamed build can both have version **1.0.5-SNAPSHOT**, but their artifact IDs differ. NiFi stores the full processor type and bundle coordinates with the flow. Replacing a NAR or using **Change version** does not convert the changed type and artifact.

The default Application URI remains `urn:nifi:opcua:subscriber`. This identifies the OPC UA client on the protocol connection. It does not make the old NiFi processor type loadable.

## Replace existing processors

Plan a maintenance window and an ingestion gap from stopping the old source until the replacement produces data. A new subscription can emit initial values again, so downstream consumers may see duplicates. Apply this procedure to every processor referencing the old bundle before removing that bundle.

1. Securely back up the flow configuration, old NAR, checksum and source commit. Record all processor properties, parameter-context references, endpoint, Tags, certificate/key/trust paths, scheduling, execution settings and auto-terminated relationships. Record connection destinations, backpressure and expiration settings, permissions, monitoring and API references. Preserve sensitive parameter references and the installation's sensitive-properties key through your normal secure backup procedure.
2. Stop the old source. Before removing it, confirm that its in-memory notifications drained successfully and its downstream queues are empty, or make an explicit decision to discard the remaining memory and queued data under your ingestion policy. Stop alone does not prove a successful drain. Check the drain outcome and remaining buffers on all cluster nodes. If draining cannot finish and data must be retained, keep the old processor and NAR available until the problem is resolved. If discarding queued data, empty the affected queues through NiFi before deleting their connections.
3. Once the drain or discard decision is complete, remove the old source's connections and processor. Keep the secure backup for rollback.
4. Stop NiFi on every cluster node. Move the old NAR outside every configured NAR library and autoload directory. Install the matching new NAR on every node, using identical bytes and one copy of its coordinates across all scanned directories. Record its checksum and source commit, then restart NiFi.
5. Add `ConsumeOPCUA` from bundle `io.github.paf91.nifi:nifi-opcua-nar:1.0.5-SNAPSHOT`. Restore the recorded properties, parameter references, scheduling, execution settings and auto-terminated relationships. Recreate the connections with their recorded settings and reconnect every used relationship. The processor and connections have new IDs. Update permissions, monitoring and API references to those IDs.
6. Check validation and run Verify. Start the new source and confirm fresh values, quality, timestamps, expected routing and the absence of unexpected bulletins.

Node-local pending memory and custom counters are not migrated. The processor does not recover history or provide lossless failover or exactly-once delivery. Do not edit encrypted flow files with a blind text replacement. See [installation](installation.md) for artifact selection and later replacements of builds with identical new bundle coordinates.

## Rollback

1. Stop `ConsumeOPCUA`. Successfully drain its in-memory notifications and all new downstream queues, or explicitly decide to discard their remaining data before removing the new source and connections. If discarding queued data, empty the affected queues through NiFi before deleting their connections.
2. Stop NiFi on every cluster node. Move the new NAR outside scanned directories, restore the matching old NAR on every node, and restore a compatible flow backup through the installation's recovery procedure. The backup must reference the old processor type and bundle, with the matching sensitive-properties key and parameter contexts.
3. Restart NiFi, check validation and connections, run Verify, then start and confirm fresh data. Update external references if the restored IDs differ.

The old NAR cannot resolve `ConsumeOPCUA`. A NAR rollback alone does not convert the flow or recover lost notifications. Keep flow backups and NiFi repositories consistent. Do not roll repositories back independently or delete working directories and caches as a migration shortcut.

## Earlier private-preview namespace

The private preview used Maven group `ai.paf91.nifi` and processor type `ai.paf91.nifi.opcua.SubscribeOpcUa`. The earlier move to `io.github.paf91.nifi` was a separate namespace change. The current NAR provides neither old type. For a private-preview flow, follow the same replacement procedure using its actual old bundle and a compatible rollback backup.
