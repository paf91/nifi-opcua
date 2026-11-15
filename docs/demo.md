# Fresh local demo

The demo runs NiFi 1.23.2 on Java 8 and a Milo OPC UA server using generated certificates, Basic256Sha256 / SignAndEncrypt, and test username/password authentication. It exposes a toggling Boolean at `ns=2;s=Demo.Boolean` plus standard server time.

Use a fresh working copy with its own `.env` and `demo/runtime/security` directory. The commands below use the separate Compose project `nifi-opcua-consume-preview`, with no retained NiFi volumes. Do not rebuild or restart an existing persisted preview with the renamed NAR before completing the [processor migration](namespace-migration.md).

Requirements: Docker Engine with Compose v2, Maven 3.9.11, JDK 8, Python 3.9+, and enough memory for NiFi (configured heap: 512 MiB–1 GiB, plus native/container overhead). Port `18080` and the selected Docker subnet must be free. The seeder uses that fixed port. Use another Docker host if a retained preview occupies it. Run commands from the fresh working copy's repository root.

## Build and start

```sh
mvn -B -ntp clean verify
mvn -B -ntp -f demo/pom.xml clean package
python3 scripts/init-demo.py
docker compose -p nifi-opcua-consume-preview up -d --build
```

The development NAR is `nar/target/nifi-opcua-1.23.2-1.0.5-SNAPSHOT.nar`. The initializer creates `.env` only if absent and preserves the existing sensitive-properties key. Do not regenerate that key while reusing this preview's NiFi volumes. The demo server runs as UID/GID 1000. Ensure this working copy's `demo/runtime/security` is writable by that account, and do not share it with the retained preview. Configure ownership for your Docker host rather than making keys world-readable.

Wait until **http://localhost:18080/nifi/** responds, then:

```sh
python3 scripts/seed-preview.py
```

This creates a demo process group once, using `ConsumeOPCUA` for the source. Re-running the seeder does not overwrite an existing flow. That guard does not make an old persisted `SubscribeOpcUa` flow compatible with the new NAR or migrate it. The returned URL opens the created group.

## Inspect values

1. Configure the source and inspect **Tags** and the connection properties.
2. Enable and start the source. The downstream LogAttribute processor remains stopped.
3. Right-click the connection → **List queue** → inspect a FlowFile's content and attributes.
4. Stop the source when finished. Start the sink to drain the queue or explicitly empty the demo queue if its records are disposable.

The source is initially disabled. The queue expires demo FlowFiles after ten minutes and has backpressure limits. It is not a storage example.

## Stop and resume this preview

```sh
docker compose -p nifi-opcua-consume-preview stop
docker compose -p nifi-opcua-consume-preview start
```

Configuration and repositories remain in this project's named volumes. `docker compose -p nifi-opcua-consume-preview down` removes its containers/network but preserves named volumes unless `--volumes` is explicitly supplied. Deleting volumes is a destructive reset, not a routine shutdown. Preserve the matching `.env` and generated security directory when retaining volumes.

## Boundaries

Only the NiFi port is published, bound to `127.0.0.1:18080`. This HTTP UI has no login and is for local development. The OPC server is internal to the Compose network. The public test login is `demo` / `demo-password`. Never reuse it or generated demo keys for production.

The default subnet is `172.30.252.0/24`. If it conflicts with an existing network, set `NIFI_DEMO_SUBNET` in this copy's `.env` before creating the network. The preview image expects the legacy NAR in `nar/target`. Rebuild legacy after building the modern profile.

Keep the explicit `-p nifi-opcua-consume-preview` option for every Compose command in this guide. It separates the new preview's named volumes from the retained `nifi-opcua-preview` project. The local Maven artifacts use `nifi-opcua-*` names. Remote repository names and URLs remain unchanged.
