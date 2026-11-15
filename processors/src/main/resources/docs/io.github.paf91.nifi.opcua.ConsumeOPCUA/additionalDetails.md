# ConsumeOPCUA configuration examples

## ConsumeOPCUA

ConsumeOPCUA subscribes to OPC UA value changes and writes notifications to FlowFiles.

## Configuring tags

Use these examples to configure aliases and complete NodeIds. Replace the addresses with those supplied by your OPC UA server administrator or address-space browser. The included demo server exposes different nodes.

| Alias | NodeId | Server data type |
|---|---|---|
| Temperature | ns=2;s=Example.Tank.Temperature | Float |
| Pressure | ns=2;s=Example.Pipe.Pressure | Double |
| PumpRunning | ns=2;s=Example.Pump.Running | Boolean |

### Bulk list

Paste one entry per line into **Tags**. Each entry may be a complete NodeId or an alias followed by a vertical bar and a NodeId.

```text
Temperature | ns=2;s=Example.Tank.Temperature
Pressure | ns=2;s=Example.Pipe.Pressure
PumpRunning | ns=2;s=Example.Pump.Running
```

Use JSON when individual tags need different sampling intervals or monitored-item queue sizes:

```json
[
  {"alias":"Temperature","nodeId":"ns=2;s=Example.Tank.Temperature","samplingIntervalMs":1000,"queueSize":100},
  {"alias":"PumpRunning","nodeId":"ns=2;s=Example.Pump.Running","samplingIntervalMs":250,"queueSize":200}
]
```

### Individual properties

Alternatively, add a dynamic property named **PumpRunning** with value `ns=2;s=Example.Pump.Running`. Do not configure the same alias or NodeId twice across the bulk list and individual properties.

### Namespace URIs and deadbands

Numeric addresses such as `ns=2;s=Temperature` keep using index 2. URI addresses such as `nsu=urn:example:minute;s=Temperature` resolve the server's NamespaceArray for each new session. Use the exact namespace for the intended data model; indexes do not define data frequency. A different session causes a full connection rebuild when namespace-URI tags or selected cached metadata need refreshing, before accepting more values. Missing URIs or duplicate resolved NodeIds fail the connection. Encode semicolons and percent signs in the URI as `%3B` and `%25`.

```json
{"nodeId":"nsu=urn:example:minute;s=Temperature","deadbandType":"Absolute","deadbandValue":0.5}
```

This example filters changes to a tag with a numeric value. Deadband types are None, Absolute and Percent. Absolute requires a finite nonnegative value; Percent requires 0–100 and the server's EURange property. Quality changes still produce notifications. A rejected filter fails the subscription; it is not silently removed.

### Connection verification and monitoring

NiFi Verify opens a temporary session and subscription to check discovery, authentication/trust, namespace resolution and all configured monitored items. It closes them afterwards and emits no measurements. It may consume an additional server session. Results include endpoint choices, certificate expiry/fingerprints and revised settings. Verify captures an independent snapshot and does not refresh the running connection's periodic snapshot.

Set Diagnostics Interval (seconds) above zero to emit JSON objects on the diagnostics relationship. The default is zero (disabled), and the relationship is automatically terminated unless routed. Measurements use the configured JSON Array or NDJSON format. Reports include session activity, keep-alive and notification receipt times, negotiated settings, namespaces, buffer usage and cumulative drops. A quiet tag is not necessarily disconnected. Reports can be delayed by NiFi backpressure, and counters reset with the processor instance. Metadata details are capped.

settingsMayBeStale means Milo activated a session with a different ID from this connection's first session, so the setup-time namespace and negotiated-settings snapshot may be stale. Subscriptions without namespace-URI tags or selected cached metadata can continue through SDK transfer or recreation. Namespace-URI tags or selected cached metadata cause the processor to request a full connection rebuild before accepting more values.

### Types and connection settings

Data types are read from the server; do not append Float, Double or Boolean to the NodeId. Obtain the endpoint URL and the server's security/authentication settings separately. Defaults remain Basic256Sha256, SignAndEncrypt and Username/Password. For secured channels, supply a client identity. Full and Certificate validation also require server trust. Leave Application URI empty to derive it from the client certificate.

### Client identities and truststores

Client Identity Format defaults to PEM/DER: configure Client Certificate File and Client Private Key File. Select JKS or PKCS12 to use Client Keystore File instead, with a private key and its X.509 certificate chain. Configure only the active source's fields. Client Key Alias may be omitted for a store with exactly one private-key identity; multiple identities require an explicit alias. Certificate-only and secret-key aliases cannot identify the client. All identity formats retain the certificate validity, RSA size, Application URI and key-match checks.

Client Keystore Password, Server Truststore Password and Private Key Password are independently optional. Unset means an empty password, and passwords are not trimmed. Private Key Password never inherits the store password: configure both fields when they use the same password. Wrong passwords fail without retrying with integrity verification disabled. Use sensitive NiFi parameters.

Server Trust Source defaults to PKI Directory. Select JKS or PKCS12 to load trusted-certificate entries from Server Truststore File. Certificates attached only to private-key entries do not become trust anchors. In store mode, optional PKI Directory supplies issuer certificates and both CRL lists; its trusted/certs entries are ignored. NiFi 1 uses issuers/certs and issuers/crl; NiFi 2 uses issuer/certs and issuer/crl. Both use trusted/crl. Required revocation checks remain active, and stores are never modified.

PKCS12 loading uses the JVM's keystore provider. Passwordless stores with no MAC and unencrypted certificate contents are supported, including keys stored as empty-password shrouded keys. Plain unencrypted PKCS12 private-key bags are unsupported by the tested JDK providers; use PEM/DER or re-export as a shrouded key. Encrypted PKCS#8 DER files remain unsupported. Verify imported stores on the actual NiFi/JDK target.

After successful loading, a provider view with zero truststore entries can mean an empty store or PKCS12 certificates without Java trusted-entry markers. A nonempty store view without trusted certificates is reported separately. Password and integrity failures are separate loading errors. Use the NiFi JDK's keytool to import an approved server or CA certificate as a trusted entry. For example:

```sh
keytool -importcert -alias opcua-server -file server.crt \
  -keystore server-trust.p12 -storetype PKCS12
```

Verify the certificate's identity before confirming the import. The command prompts for the store password. OpenSSL's `pkcs12 -jdktrust` can export Java trusted entries when the option is available. Check `openssl pkcs12 -help`: it was available in tested OpenSSL 3.5.9 but absent in 3.0.2.

Store files are read for each new processor connection. Ordinary Milo reconnects do not guarantee a reload. Stop the source, atomically replace files on every eligible NiFi node, update password/alias properties, run Verify, then Start. Retained records keep their original delivery settings until drained.

### Server certificate validation

**Server Certificate Validation** defaults to **Full**: certificate trust, signatures, validity, usage, revocation, Application URI and endpoint hostname/IP are checked. **Certificate** skips only hostname/IP matching; the selected trust source must still contain approved server certificates or CAs, and the server's Application URI must still match its certificate URI SAN. For a literal IP endpoint, Full expects that IP in an IPAddress SAN. Fixing the server certificate or advertised endpoint is preferred.

**None (insecure)** bypasses the server certificate validator and does not open PKI Directory or a truststore. It permits server impersonation, even if channel traffic is encrypted. It does not disable channel/session signatures, client certificate/key requirements, or server-side user authentication. Java can still reject a certificate whose critical key-usage extension forbids signing when verifying the server's session signature. These modes also apply when an unsecured channel uses encrypted username tokens. No mode automatically adds certificates to the trust list. A None/None channel with anonymous access or an unencrypted username token does not authenticate the server through a certificate, even with Full selected.

Modern Milo 1 compares the discovery endpoint list with the endpoints returned by CreateSession. Legacy Milo 0.6 lacks that comparison, including on SignAndEncrypt channels. Certificate validation and applicable channel/session signatures still apply but do not replace the missing comparison.

Stop, edit and restart to change the mode. Verify and diagnostics report the mode; successful verification with None does not prove server identity. Application URI always identifies this client; do not set it to the server's URI.

### Endpoints without channel security

For username/password without a client certificate, set **Security Policy** to `None`, **Security Mode** to `None`, and **Authentication** to `Username/Password`. Supply credentials and Tags; leave client identity fields unset. Supported encrypted username tokens take priority over unencrypted tokens. Full and Certificate validation require the selected server trust source to trust the certificate used for token encryption. If the selected token has no encryption, the password-protection setting below determines whether it is allowed.

For anonymous access, select `Anonymous` and leave Username/Password unset. Anonymous on a secured channel still requires the client certificate/key and, with Full or Certificate validation, server trust. Without channel security, Application URI defaults to `urn:nifi:opcua:subscriber`. Endpoint selection never downgrades the requested settings.

### Password protection

**Allow Unencrypted Password** defaults to **None channels only** and has no effect on anonymous login. It controls whether the password may be sent without encryption on the OPC UA connection. The password can be encrypted separately in the username token or protected by a SignAndEncrypt channel.

| Channel | Effective username-token policy | None channels only (default) | Never | Always (insecure) |
|---|---|---|---|---|
| Any supported channel | Supported RSA encryption | Allowed | Allowed | Allowed |
| None / None | None | Allowed | Rejected | Allowed |
| Non-None / Sign | None | Rejected | Rejected | Allowed |
| Non-None / SignAndEncrypt | None | Allowed | Allowed | Allowed |

A missing or empty token-policy URI inherits the channel policy. Selection prefers an encrypted token using the channel policy, then another supported RSA token, then an unencrypted token if allowed. The preference applies under every setting. Always (insecure), stored as `Always`, permits deliberate plaintext compatibility on Sign. It does not force plaintext or retry it after certificate validation fails.

When upgrading an earlier development build, check Sign channels offering only a None-policy username token: the default now rejects them. Configure an encrypted token or SignAndEncrypt, or select Always (insecure) for deliberate plaintext compatibility. A None channel offering both encrypted and unencrypted tokens now selects encryption regardless of advertised order. With Full or Certificate, configure server trust before starting.

### Included demo

The local demo exposes `DemoSwitch | ns=2;s=Demo.Boolean`. Its addresses differ from the examples above.

### Delivery limits

Stop closes the subscription before attempting to commit buffered notifications. **Stop Drain Timeout (ms)** defaults to 5000; zero disables the attempt. The drain respects downstream backpressure, primary ownership and batch limits. Failure, interruption or expiry ends the wait. Only remaining uncommitted records produce a warning, and they remain available for Start. An already-submitted commit may complete later; its batch stays reserved until the outcome is known. Stop-time I/O uses at most one daemon worker per processor. The timeout bounds the extra drain wait, not active NiFi tasks or uninterruptible repository I/O.

Notifications are buffered in memory. Stop/Start in the same JVM retains pending records; a JVM restart loses them. Buffers are local to each node. A full buffer drops new notifications before encoding. Remaining byte capacity also bounds encoding output. Defaults are 64 MiB of serialized buffered data, 1 MiB per record, and 4 MiB per success FlowFile. Count limits also apply. Maximum Record Bytes must fit the buffer and Maximum FlowFile Bytes minus two; batches account for brackets/commas or NDJSON newlines. Oversized values are counted separately when the record-size limit is reached; capacity-based rejection counts as overflow. These are serialized-data limits, not total JVM memory limits. Retained records use their original byte limits until drained. Primary-node failover can produce gaps or duplicates; the processor does not recover historical values.

Version 1.0.3 streams ExtensionObject bodies as objects with encoding, encodingId (binary/XML) or typeId (JSON), and body. Binary bodies use Base64; XML and JSON bodies are quoted strings. Other unsupported objects contain encoding: unsupported and their Java type without display text. Consumers of earlier structured-value display text must migrate. Both adapters emit multidimensional arrays as nested JSON arrays. The modern adapter streams Milo's flat matrix representation without first constructing nested arrays. Legacy Milo already supplies nested arrays. Cancellation fences final queue admission and closes the transport independently of value encoding and repository writes; NiFi can still wait for a stalled repository operation before completing Stop.

### Output fields and ETL options

**Output Fields** selects a comma-separated subset of tag, nodeId, configuredNodeId, namespaceUri, value, valueType, statusCode, quality, sourceTimestamp, receivedTimestamp, serverTimestamp, dataType and engineeringUnits. Include value and tag or nodeId. Defaults preserve the existing schema. A compact selection is `tag,value,statusCode,sourceTimestamp`. Omitted fields are not formatted. Keep statusCode when overflow/detail bits matter; quality alone does not preserve them.

**Output Format** supports JSON Array (default) and NDJSON (one object and newline per record, application/x-ndjson). **Timestamp Format** supports ISO-8601 (default) and Epoch Milliseconds for notification timestamps. Milliseconds discard finer precision; DateTime values inside value remain ISO. Missing/zero timestamps are null. Selecting serverTimestamp adds the server's timestamp.

**Route by Quality** defaults to false. When enabled, connect or explicitly terminate bad and uncertain: Bad goes to bad, Uncertain/missing status to uncertain, and Good to success. Routing uses the original status, including when status fields are omitted. Batches contain contiguous records for one route; records are not reordered.

**Maximum Batch Delay (ms)** defaults to zero, disabling the delay. A positive value allows a partial batch to accumulate from the oldest queued record's arrival. Count/byte limits, route boundaries, buffer pressure and Stop can make a batch ready sooner. Scheduling/backpressure can extend actual latency. Active triggers wait at most 50 ms for a commit or batch; OPC UA callbacks never wait for storage or batch completion.

Selecting dataType and/or engineeringUnits reads optional metadata during connection setup, in adaptive batches of up to 100 operations with one Request Timeout budget. Missing, invalid, oversized or unreadable metadata becomes null and is summarized in diagnostics. Cached strings are at most 1024 characters; only standard EUInformation encodings are decoded, with a 16 KiB binary-body or 16384-character XML/JSON limit. Metadata refreshes on a new session, not every notification. It can remain stale if changed within an existing session. Selecting dataType provides an OPC UA NodeId; valueType remains a Java/Milo representation name.

Retained records keep their original field selection, framing, timestamp format and quality routing until committed, including stop-time draining. Output attributes describe these settings. Keep old quality connections until buffered records drain. Field projection reduces JSON and downstream traffic; sampling and deadbands control incoming OPC UA traffic.
