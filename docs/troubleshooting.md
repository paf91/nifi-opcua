# Troubleshooting

Start with `logs/nifi-app.log`, processor bulletins, validation errors and queue/backpressure status. Capture the NAR checksum, NiFi/JDK versions, sanitized endpoint and time of failure. Do not attach private keys, credentials, raw production flows or proprietary tag inventories to public issues.

| Symptom | Check |
|---|---|
| ConsumeOPCUA is missing | Release 1.0.4 provides only SubscribeOpcUa. Check for the development `nifi-opcua-nar` bundle on every cluster node, its checksum, library/autoload paths and NAR loading errors, then refresh the UI |
| SubscribeOpcUa is unresolved after installing the new NAR | The current NAR provides only ConsumeOPCUA. Follow the [processor migration](namespace-migration.md). Change version does not convert the changed processor type and artifact |
| Rebuilt SNAPSHOT still behaves like the previous build | For identical `nifi-opcua-nar` coordinates, Change version cannot distinguish builds. Follow the [maintenance replacement procedure](installation.md#replacing-a-build-with-the-same-bundle-coordinates), keep backups outside all scanned directories and restart NiFi |
| ConsumeOPCUA is unresolved after rollback | Drain or explicitly discard any remaining downstream queued data, then restore a flow backup compatible with the old NAR using the [rollback procedure](namespace-migration.md#rollback). Notifications left in processor memory were lost when NiFi restarted. |
| Seeder reports an existing demo but its source is unresolved | The seeder does not migrate existing flows. Migrate the persisted preview before using the new NAR, or follow the [fresh, separate demo instructions](demo.md) |
| UnsupportedClassVersionError | Wrong NAR/JVM combination; modern needs Java 21 |
| Processor is restricted | Administrator authorization for filesystem read/write restricted components |
| Invalid Tags | Complete parseable NodeIds; unique aliases/NodeIds; JSON keys and numeric ranges |
| File not found / access denied | Absolute paths inside the NiFi host/container; service account permissions |
| Certificate URI error | During Loading client identity: client certificate must have exactly one absolute URI SAN; leave Application URI unset or match it exactly. During Opening authenticated session: also check that the server's advertised Application URI matches its server certificate URI SAN |
| Certificate/key mismatch | Use the matching RSA private key and leaf certificate, leaf first in a PEM chain |
| Bad_CertificateUntrusted | The OPC server must trust the client identity on secured channels. NiFi must trust the approved server certificate or CA through the selected Server Trust Source |
| `SunCertPathBuilderException: unable to find valid certification path to requested target` | Java could not build a path from the server certificate to an approved trust anchor. Check the effective trust source and intermediate certificates. See [server certificate path](#server-certificate-path) |
| Bad_CertificateHostNameInvalid | Host in the selected server-advertised endpoint must match its certificate DNS/IP SAN; a literal IP needs an IPAddress SAN. Certificate validation mode skips only this match while retaining trust and Application URI checks |
| Expiry or revocation failure | Clock, certificate validity and required issuer certificates/CRLs; Certificate mode retains these checks |
| Bad_UserAccessDenied / identity rejection | Username/password and access rights on the server; certificate trust alone is not user authorization |
| No OPC TCP endpoint for configured settings | Match Security Policy, Security Mode and Authentication to a server endpoint; selection never downgrades automatically |
| Username token rejected by Allow Unencrypted Password | Use an encrypted username token or SignAndEncrypt. The Never setting rejects plaintext tokens on None/None and Sign; the default None channels only rejects them on Sign. Always (insecure) permits deliberate plaintext compatibility. Certificate validation does not control this rejection |
| None/None login asks for server trust | A supported encrypted username token is preferred even when plaintext is also advertised. With Full or Certificate validation, trust the server certificate through the selected Server Trust Source. No client identity is needed, and a trust failure does not cause a plaintext retry |
| Truststore exposes no entries after loading | The store may be empty, or PKCS12 certificates may lack Java trusted-entry markers and be invisible to the JVM provider. Check with the NiFi JDK and import approved certificates with keytool. A visible store containing only private-key entries also supplies no trust anchors. Password/integrity failures are separate loading errors. See [server trust sources](configuration.md#server-trust-sources) |
| Anonymous configuration asks for client certificate | Sign and SignAndEncrypt still authenticate the client application; choose None/None only for an endpoint that offers it |
| Connection to an unexpected hostname | OPC discovery may advertise another host; fix DNS/network/server configuration |
| RUNNING but no data | Bulletins, endpoint reachability, primary role, outbound backpressure and node permissions |
| Only initial values | Subscription emits changes, not periodic snapshots of unchanged values |
| Bad/Uncertain values | Inspect statusCode and the OPC server's node quality; those values are emitted deliberately |
| Growing queue | Downstream flow stopped or slower than source; stop source when not needed |
| Drop counter increasing | Local buffer overflow or encoding failure; inspect value sizes, rate and downstream throughput |
| Old values after Start | Pending memory records drain first, retaining old source timestamps and endpoint |
| Unexpected initial values after reconnect | New subscriptions can send current values again; no built-in deduplication |
| Demo cannot start | Missing legacy NAR/demo classes, Docker subnet conflict, UID 1000 permissions, or lost .env key |

To isolate a server issue, begin with one known readable NodeId and verified certificates. Add tags gradually. An invalid monitored item fails the connection attempt; retries will not fix a wrong NodeId. Prefer correcting server certificates/configuration. For an explicit hostname compatibility exception, use Server Certificate Validation = Certificate. None bypasses server certificate validation and permits impersonation; it does not override the server's rejection of a client certificate or credentials. See the [validation modes](configuration.md#certificates-and-trust).

For discovery failures, supply the server's advertised discovery URL, including its endpoint path. The client may try a `/discovery` suffix after a failed service attempt. Individual operations are bounded; multiple discovery/setup operations can take longer than a single Request Timeout.

If NiFi cannot decrypt existing flow properties after recreating a demo container, restore the original matching `.env` key. Do not overwrite encrypted configuration or delete repositories to hide the error.

## Server certificate path

1. Keep **Server Certificate Validation = Full** and run **Verify**. Confirm the selected endpoint and compare its advertised leaf SHA-256 fingerprint with the current fingerprint obtained independently from the server administrator. A path-building failure can occur with a matching hostname. **Certificate** skips the hostname/IP check and still requires the same trust path.
2. Check **Server Trust Source** and its configured path on each eligible NiFi node or inside its container. With **PKI Directory**, place the independently approved root or directly trusted server certificate in its own file in `trusted/certs/`. With **JKS** or **PKCS12**, check that certificate is a trusted-certificate entry in **Server Truststore File**. Verify reports how many trusted certificates Milo loaded. A positive count can include an unrelated CA.
3. If the server omits an intermediate, have the server administrator supply it in the chain or configure **PKI Directory** and place it in `issuers/certs/` on NiFi 1 or `issuer/certs/` on NiFi 2. Inspect a supplied PEM bundle in file order with `openssl crl2pkcs7 -nocrl -certfile bundle.pem | openssl pkcs7 -print_certs -noout`. For example, with directory trust, a root followed by a leaf in one file under `trusted/certs/` registers only the root. Reversing them registers only the leaf. A file directly in the PKI root supplies neither. When you trust a CA-issued leaf directly, keep that leaf in the selected trust source. Put its issuing root in the issuer directory, along with any intermediates needed to complete the chain. Both Milo versions needed the local issuing root for this case even when the peer sent it. A directly trusted self-signed leaf has no separate issuer.
4. Supply any required current CRLs in `trusted/crl/` and the corresponding `issuers/crl/` or `issuer/crl/` directory. Run Verify again and check the session and monitored-item results. [Trust-source layout](configuration.md#server-trust-sources)
