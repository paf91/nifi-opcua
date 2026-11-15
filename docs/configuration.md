# Configuration reference

[Documentation index](../README.md#documentation)

| Property | What to supply |
|---|---|
| Endpoint URL | Server discovery URL, e.g. `opc.tcp://server:4840/endpoint` |
| Security Policy | None, Basic128Rsa15, Basic256, Basic256Sha256 (default), Aes128_Sha256_RsaOaep, Aes256_Sha256_RsaPss |
| Security Mode | None, Sign, SignAndEncrypt (default) |
| Authentication | Username/Password (default), Anonymous |
| Allow Unencrypted Password | None channels only (default), Never, Always (insecure) |
| Server Certificate Validation | Full (default), Certificate (skip hostname/IP only), None (insecure) |
| Application URI | Optional override; derived from the client certificate for secured channels, otherwise defaults to `urn:nifi:opcua:subscriber` |
| Client Identity Format | PEM/DER (default), JKS or PKCS12 |
| Client Certificate File | With PEM/DER: absolute path to PEM certificate chain, leaf first, or a DER leaf certificate |
| Client Private Key File | With PEM/DER: absolute path to RSA PEM key (PKCS#1/PKCS#8) or unencrypted PKCS#8 DER |
| Client Keystore File | With JKS/PKCS12: absolute path to the store containing the client private key and certificate chain |
| Client Keystore Password | Optional sensitive store password; unset means an empty password |
| Client Key Alias | Optional private-key alias; required when the store has multiple private-key identities |
| Private Key Password | Optional sensitive password for the PEM key or selected store key; independent of the store password |
| Server Trust Source | PKI Directory (default), JKS or PKCS12 |
| Server Truststore File | With JKS/PKCS12: absolute path to the store containing trusted-certificate entries |
| Server Truststore Password | Optional sensitive store password; unset means an empty password |
| PKI Directory | Server trust in directory mode; optional issuer/CRL material in store mode |
| Username / Password | OPC UA user credentials; Password is sensitive |

## Security and authentication

These are OPC UA security settings for `opc.tcp`, independent of HTTPS/TLS on the NiFi UI. Match the endpoint advertised by the server; discovery never silently downgrades the selected policy, mode or authentication.

| Channel | Authentication | Client identity | Server trust | Credentials |
|---|---|---|---|---|
| None / None | Anonymous | Not needed | Not needed | Not needed |
| None / None | Username/Password, unencrypted token | Not needed | Not needed | Username and Password |
| None / None | Username/Password, encrypted token | Not needed | Required for the server certificate | Username and Password |
| Any supported non-None policy / Sign | Anonymous or Username/Password | Required | Required | Only for Username/Password |
| Any supported non-None policy / SignAndEncrypt | Anonymous or Username/Password | Required | Required | Only for Username/Password |

The server-trust requirements above apply to **Full** and **Certificate** validation. Choose a PKI directory or JKS/PKCS12 truststore with **Server Trust Source**. **None** bypasses server certificate validation and does not open either trust source. It does not remove the selected channel's client-identity or login requirements.

**Username/password without a client certificate:** select None, None and Username/Password, set your credentials and Tags, and leave client identity fields unset. The server advertises token protection separately from channel protection. Supported encrypted username tokens take priority over unencrypted tokens. With Full or Certificate validation, an encrypted token requires the selected server trust source to trust the server certificate. This protects the password token while channel data remains unencrypted. A client certificate is not required.

**Anonymous without certificates:** select None, None and Anonymous. Set Endpoint URL and Tags. Leave certificate/key/PKI and username/password unset.

**Secured channels:** the existing Basic256Sha256 / SignAndEncrypt / Username/Password defaults are unchanged. Anonymous login on a secured channel still requires the application certificate. Full and Certificate validation require server trust as well. Sign authenticates messages without encrypting their contents. None with an unencrypted username token sends credentials without encryption; choose it only when that matches the intended network configuration. Basic128Rsa15 and Basic256 are explicit legacy compatibility options. This processor supports the six listed RSA/None policies, not every OPC UA profile or identity-token type; X.509 user authentication, issued tokens and HTTPS transport are outside this implementation.

Policy None requires mode None. Other policies require Sign or SignAndEncrypt. Unavailable policy/mode/login combinations fail with a configuration-specific message. Stop, edit and restart to apply settings. Old buffered records keep their original endpoint and security attributes until drained.

### Password protection

**Allow Unencrypted Password** controls whether a username/password login may send its password without encryption on the OPC UA connection. The password can be encrypted separately in the username token or protected by a SignAndEncrypt channel. The setting has no effect on anonymous login.

| Channel | Effective username-token policy | None channels only (default) | Never | Always (insecure) |
|---|---|---|---|---|
| Any supported channel | Supported RSA encryption | Allowed | Allowed | Allowed |
| None / None | None | Allowed | Rejected | Allowed |
| Non-None / Sign | None | Rejected | Rejected | Allowed |
| Non-None / SignAndEncrypt | None | Allowed | Allowed | Allowed |

A missing or empty token-policy URI inherits the channel policy. For each matching endpoint, selection prefers an encrypted token using the channel policy, then another supported RSA token, then an unencrypted token if allowed. This preference applies under every setting, including Always. Unsupported token policies are skipped. A certificate-validation failure for the selected encrypted token ends the attempt without falling back to plaintext.

In the NiFi UI, **Always (insecure)** has the stored value `Always`. It permits plaintext credentials on a Sign channel and does not force plaintext when an encrypted token is available. **Never** still accepts a None-policy token on SignAndEncrypt because the channel encrypts it.

### Password-protection migration

Check these cases when replacing an earlier development build:

- A Sign channel offering only a None-policy username token now fails with the default None channels only setting. Configure an encrypted username token or SignAndEncrypt. To retain deliberate plaintext compatibility on Sign, select Always (insecure).
- A None channel offering both encrypted and unencrypted username tokens now selects encryption regardless of token order. With Full or Certificate validation, configure the selected server trust source before starting. Failed trust does not cause a plaintext retry, even with Always.

## Certificates and trust

Use sensitive NiFi parameters for passwords. Paths refer to the **NiFi server filesystem**, not your browser's filesystem. The NiFi service account needs read access to identity/truststore files and permission to initialize and maintain a configured PKI directory. When channel security is enabled, the certificate must be valid, have an RSA key of at least 2048 bits, and match the private key. Its single URI subject alternative name identifies this OPC UA client; it is not the server endpoint. Application URI is derived from that certificate, and any explicit override must match.

### Client identity files and stores

**Client Identity Format = PEM/DER** retains the existing certificate/key file pair. Select **JKS** or **PKCS12** to use **Client Keystore File** instead. For secured channels, configure only the fields for the selected source: PEM/DER rejects keystore-specific fields, and a store identity rejects separate certificate/key files. A None/None channel does not load a client identity.

A store identity must contain a private key and a nonempty X.509 chain, with the client leaf first. Leave **Client Key Alias** unset only when the store contains exactly one private-key identity. If there are multiple, set the alias explicitly. Certificate-only and secret-key aliases cannot identify the client. The same RSA, validity, Application URI and key-match checks apply to files and stores.

Store and key passwords are independently optional and are never trimmed. An unset password supplies an empty password. **Private Key Password** does not inherit **Client Keystore Password**: if both use the same password, configure the same sensitive parameter in both fields. This also allows an empty-password key inside a password-protected store. A wrong password fails loading without retrying with integrity verification disabled.

PKCS12 loading uses the JVM's keystore provider. Passwordless stores with no MAC and unencrypted certificate contents are supported, including client keys stored as empty-password shrouded keys. Plain, unencrypted PKCS12 private-key bags are unsupported by the tested JDK providers; use PEM/DER or re-export the key as a shrouded key. Encrypted PKCS#8 DER files remain unsupported. Store encryption algorithms must be available in the target JVM, so verify imported stores on the actual NiFi/JDK target.

### Server trust sources

The server trust source is separate from client identity and user credentials. With Full or Certificate validation on a secured channel, the OPC UA administrator must trust the client certificate, and NiFi must trust the server certificate or its CA.

**Server Trust Source = PKI Directory** uses the existing directory trust layout. Populate it before starting. The issuer-directory name depends on the build target:

| Material | NiFi 1 / Milo 0.6 | NiFi 2 / Milo 1 |
|---|---|---|
| Approved server / trusted CA certificates | `trusted/certs/` | `trusted/certs/` |
| Trusted CA revocation lists | `trusted/crl/` | `trusted/crl/` |
| Intermediate CA certificates | `issuers/certs/` | `issuer/certs/` |
| Intermediate CA revocation lists | `issuers/crl/` | `issuer/crl/` |

All paths in this table are relative to **PKI Directory**. The PKI root and these locations must be directories; an ordinary file at one of these paths causes initialization to fail. Existing directory symlinks are allowed. When moving configuration between NAR variants, use the destination target's issuer-directory name. Clear Server Truststore File and Server Truststore Password when selecting the PKI Directory source.

**Server Trust Source = JKS** or **PKCS12** loads trusted-certificate entries from **Server Truststore File**. **Server Truststore Password** is optional; unset supplies an empty password. A store with no trusted-certificate entries fails loading. Certificates attached only to private-key entries do not become trust anchors.

After a successful store load, zero entries exposed by the JVM provider can mean an empty store or PKCS12 certificates without Java trusted-entry markers. A nonempty provider view with no trusted certificates is reported separately. Password and integrity failures occur earlier and are not treated as empty stores. Check the store with the JDK used by NiFi.

Use that JDK's `keytool` to import an approved server or CA certificate as a trusted entry, for example:

```sh
keytool -importcert -alias opcua-server -file server.crt \
  -keystore server-trust.p12 -storetype PKCS12
```

Verify the certificate's identity before confirming the import. The command prompts for the store password. OpenSSL can export Java trusted entries with `pkcs12 -jdktrust` when that option is available. Check `openssl pkcs12 -help` first: it was available in tested OpenSSL 3.5.9 but absent in 3.0.2.

In store mode, an optional **PKI Directory** supplies issuer certificates and both CRL lists using the paths above. Its `trusted/certs/` entries are ignored: the selected truststore is the sole source of trust anchors. The processor does not import store entries into the directory or write to either store. Full and Certificate keep their revocation checks, so a CA deployment may still need this PKI directory for applicable CRLs. Missing required CRLs or a certificate listed as revoked still fail validation.

**Server Certificate Validation** controls verification independently of Security Policy, Security Mode and Authentication:

| Mode | Server certificate checks | Trust source |
|---|---|---|
| Full (default) | Trust chain, signatures, validity, certificate usage, revocation, Application URI and endpoint hostname/IP | Required when the server certificate protects the channel or username token |
| Certificate | Same checks as Full, except the endpoint hostname/IP match | Same as Full; direct server-certificate trust or CA trust is supported |
| None (insecure) | Bypasses the server certificate validator, including trust, validity, hostname/IP and Application URI checks | Unused; no truststore or trust directory is opened or updated |

For `Bad_CertificateHostNameInvalid`, first correct the server's advertised endpoint or certificate SAN. A literal IP endpoint needs that IP in an IPAddress SAN; a DNS SAN resolving to that IP does not match it. **Certificate** can accommodate a known hostname/IP mismatch while retaining certificate trust and Application URI checks. The server's advertised Application URI must still match its certificate URI SAN. The processor's **Application URI** identifies the client and cannot repair a server identity mismatch.

**None permits server impersonation**, even with SignAndEncrypt: encryption alone does not establish who receives the credentials and data. It does not disable Milo's applicable channel/session signature checks, client identity checks or server-side authentication. Java can still reject a server certificate whose critical key-usage extension forbids signing when checking the server's session signature. A server rejecting the client's certificate is unaffected. Verify and connection diagnostics report the selected mode. Stop, edit and restart to change it. Existing flows default to Full.

The modern Milo 1 adapter enables comparison of the discovery endpoint list with the endpoints returned by CreateSession. The legacy Milo 0.6 adapter lacks that comparison. SignAndEncrypt does not supply the missing legacy check, and certificate validation or session signatures do not replace it.

A CA-signed chain may require its CRLs in Full and Certificate modes. Obtain certificates/CRLs from the server administrator; verify identity before trusting them. No mode automatically adds certificates to the trust list. Discovery itself may use an unsecured discovery endpoint; the selected session endpoint must still use the required security settings. Skipping hostname validation does not repair routing: if discovery advertises an unreachable hostname, fix server endpoint configuration or network/DNS routing. Full cannot provide server authentication on a None/None channel with anonymous access or an unencrypted username token, where no server certificate is used for protection.

Identity and truststore files are read when a new processor connection is constructed. Ordinary Milo reconnects can reuse that connection and do not guarantee a reload. For deterministic rotation, Stop the processor, atomically replace the files on every eligible NiFi node, update password/alias properties if needed, run Verify, then Start. See [operations](operations.md#certificate-and-truststore-rotation) for retained-buffer and cluster considerations.

NiFi administrators must grant this processor's filesystem permissions in secured deployments. It reads private-key material and initializes a configured PKI directory. Those files are never put in FlowFiles.

## Tags

Set **Tags** to one complete NodeId per line, optionally prefixed with an alias and `|`:

```text
# Blank lines and comment lines are ignored
DemoSwitch | ns=2;s=Demo.Boolean
Server time | ns=0;i=2258
```

A bare NodeId also works; its alias defaults to the NodeId. For per-tag settings, paste a JSON array:

```json
[
  {"alias":"DemoSwitch","nodeId":"ns=2;s=Demo.Boolean","samplingIntervalMs":500,"queueSize":100},
  {"alias":"Server time","nodeId":"ns=0;i=2258"}
]
```

JSON arrays may also contain plain NodeId strings. Omitted settings use processor defaults. See `examples/tags.txt` and `examples/tags.json`. Duplicate aliases or NodeIds, including duplicates across the list and individual properties, fail validation. Stop the processor before editing, then start it to subscribe to the updated list.

## Individual properties

Minimal property:

```text
Name:  DemoSwitch
Value: ns=2;s=Demo.Boolean
```

With overrides:

```json
{
  "nodeId": "ns=2;s=Demo.Boolean",
  "samplingIntervalMs": 500,
  "queueSize": 100,
  "discardOldest": true
}
```

`samplingIntervalMs` is requested sampling on the server (0 requests its fastest supported rate). `queueSize` and `discardOldest` apply to that server-side monitored item's queue, not NiFi's local buffer. Unsupported options and malformed values fail configuration validation. The server may revise requested intervals/queue sizes. The connection attempt fails if any configured monitored item cannot be created. NiFi scheduling remains active and retries according to Reconnect Delay.

Processor-wide defaults are publishing interval **1000 ms**, sampling **1000 ms**, monitored item queue **100**, local notification capacity **10000**, batch size **500**, request timeout **10000 ms**, and reconnect delay **5000 ms**. The count and byte limits both apply. The default buffer byte capacity is 64 MiB, maximum record size is 1 MiB, and maximum success FlowFile size is 4 MiB (including JSON punctuation). Diagnostics reports are disabled by default.


NodeIds are case-sensitive. Whitespace around a list entry is trimmed; punctuation within the NodeId is preserved. Namespace indexes belong to the target server and can change after reconfiguration. Obtain the complete NodeId from that server's address-space browser. This processor resolves explicit namespace URIs; it does not discover measurement tags automatically. Optional engineering-unit metadata uses an explicit property browse path.

## Required and optional properties

| Property | Required | Default / range |
|---|---|---|
| Endpoint URL | Yes | An `opc.tcp` URL; no embedded credentials, query or fragment |
| Tags | At least one bulk or individual tag | Unset |
| Security Policy | Yes | Basic256Sha256 |
| Security Mode | Yes | SignAndEncrypt |
| Authentication | Yes | Username/Password |
| Allow Unencrypted Password | Yes | None channels only; Never requires token or channel encryption; Always (insecure) also permits plaintext on Sign |
| Server Certificate Validation | Yes | Full; Certificate skips hostname/IP; None is insecure |
| Client Identity Format | Yes | PEM/DER (default), JKS or PKCS12 |
| Client Certificate File | For secured channels with PEM/DER identity | Absolute host path |
| Client Private Key File | For secured channels with PEM/DER identity | Absolute host path |
| Client Keystore File | For secured channels with JKS/PKCS12 identity | Absolute host path |
| Client Keystore Password | No | Unset means empty; sensitive |
| Client Key Alias | If the client store has multiple private-key identities | Unset selects the sole private-key identity |
| Private Key Password | No | Unset means empty; sensitive; independent of Client Keystore Password |
| Server Trust Source | Yes | PKI Directory (default), JKS or PKCS12 |
| Server Truststore File | When server trust is needed and JKS/PKCS12 is selected | Absolute host path |
| Server Truststore Password | No | Unset means empty; sensitive |
| PKI Directory | When server trust is needed with the PKI Directory source | Absolute host path; optional issuer/CRL supplement with a store source; unused with validation None |
| Username | For Username/Password | Unset |
| Password | For Username/Password | Unset; sensitive |
| Application URI | No | Certificate URI SAN, or `urn:nifi:opcua:subscriber` without channel security |
| Publishing Interval (ms) | Yes | 1000; 1–86400000 |
| Default Sampling Interval (ms) | Yes | 1000; 1–86400000 |
| Default Monitored Item Queue Size | Yes | 100; 1–1000000 |
| Notification Buffer Capacity | Yes | 10000 records; 1–1000000 |
| Notification Buffer Bytes | Yes | 67108864; 1–2147483647 serialized bytes, including pending commits |
| Maximum Record Bytes | Yes | 1048576; 1–67108864 UTF-8 JSON bytes |
| Maximum FlowFile Bytes | Yes | 4194304; 1–67108866 measurement bytes, including array framing or NDJSON newlines |
| Diagnostics Interval (seconds) | Yes | 0 disables reports; 1–86400 enables them |
| Max Records Per FlowFile | Yes | 500; 1–100000; maximum, not a target |
| Maximum Batch Delay (ms) | Yes | 0 (disabled); 0–60000; scheduling/backpressure can extend actual latency |
| Output Fields | Yes | [Default measurement fields](output.md#output-selection-and-framing); compact example: `tag,value,statusCode,sourceTimestamp` |
| Output Format | Yes | JSON Array (default) or NDJSON |
| Timestamp Format | Yes | ISO-8601 (default) or Epoch Milliseconds |
| Route by Quality | Yes | false; true exposes bad and uncertain relationships |
| Stop Drain Timeout (ms) | Yes | 5000; 0–30000; 0 disables stop-time draining |
| Request Timeout (ms) | Yes | 10000; 1–300000 per operation; optional metadata shares one budget |
| Reconnect Delay (ms) | Yes | 5000; 1–300000 |

A per-tag sampling override may be 0 (fastest supported); the processor-wide default must be positive. Per-tag queue size is 1–1000000; `discardOldest` defaults to `true`. JSON arrays may mix strings and objects. Unknown JSON options, duplicate JSON keys, duplicate aliases and duplicate normalized NodeIds are rejected.

All properties are literal values or NiFi parameter references; NiFi Expression Language is not enabled. Use sensitive parameters for sensitive fields. Every supplied file/directory path must be absolute, including inactive source fields. Files must be accessible inside the NiFi container when running in Docker. Password fields may be left unset when the selected store or key needs an empty password.

## From a tag table to configuration

These examples show how to turn a list of NodeIds into aliased tags. They are also included in the processor's embedded NiFi documentation. Replace the addresses with complete NodeIds from your server administrator or address-space browser:

```text
Temperature | ns=2;s=Example.Tank.Temperature
Pressure | ns=2;s=Example.Pipe.Pressure
PumpRunning | ns=2;s=Example.Pump.Running
```

Copy the full address, not only its display name. You do not configure `Float`, `Boolean`, or another value type: the processor uses the server's typed value. A table of NodeIds does not contain the endpoint URL, credentials or trust material; obtain those separately. The supplied demo exposes `Demo.Boolean`, not every illustrative node in this guide.

## Namespace indexes and URIs

Both formats work in bulk text, bulk JSON and individual properties. Choose the namespace that represents the intended data model; the processor does not infer a sampling rate from a namespace number. For example, a server could expose different real-time and per-minute values:

```text
Realtime | ns=2;s=Temperature
Minute | nsu=urn:example:minute;s=Temperature
```

Use the exact URI from that server's NamespaceArray or address-space browser. `nsu=urn:example:minute;s=Temperature` follows that URI even if its numeric index changes from 3 to 4 in a later session. A numeric `ns=3;s=Temperature` always addresses index 3. Neither format converts a real-time value into a minute aggregate. Integer (`i=`), string (`s=`), GUID (`g=`) and opaque Base64 (`b=`) identifiers are supported.

URIs and identifiers are case-sensitive. In the URI portion, encode a literal semicolon as `%3B` and a literal percent sign as `%25`; a plus sign remains a plus sign. Missing or ambiguous URIs fail the connection. Mixed numeric/URI entries that resolve to the same NodeId also fail. URI-configured subscriptions are recreated when Milo opens a different session, before accepting values under a potentially changed namespace table. Buffered records retain their original resolved numeric NodeIds.

## Deadbands

Configure a server-side filter for a tag with a numeric value using JSON:

```json
{"nodeId":"nsu=urn:example:minute;s=Temperature","deadbandType":"Absolute","deadbandValue":0.5}
```

`deadbandType` is `None` (default), `Absolute`, or `Percent`. Absolute values must be finite and nonnegative. Percent values must be between 0 and 100 and require a server-provided `EURange` property. Absolute/Percent require `deadbandValue`; None accepts only zero or an omitted value. A change must exceed the threshold relative to the last queued value. Quality changes still generate notifications. Deadbands operate on the server; they do not create periodic snapshots or guarantee a maximum notification rate. If the server rejects the filter, the entire subscription attempt fails with the item status; there is no silent fallback.

## Byte limits and diagnostics

`Maximum Record Bytes` must fit `Notification Buffer Bytes`, and must be at most `Maximum FlowFile Bytes` minus two array-bracket bytes. A batch also accounts for commas. Oversized values are rejected during JSON serialization and counted separately in diagnostics. Full count or byte capacity drops the newest notification. Stop/Start retains the old buffer's byte and record limits until it drains, so reducing limits cannot strand previously accepted records. These limits bound serialized records, not the full JVM, decoded Milo values, transport buffers or temporary encoding allocations.

Use NiFi **Verify** to test discovery, trust/authentication, namespace resolution and creation of all configured monitored items. Verification briefly creates a real session and subscription, then closes them; it emits no measurement FlowFiles and does not drain or replace the running processor's buffer. It can consume an additional server session while the processor is running. See [diagnostics](diagnostics.md) for optional health reports and their interpretation.

## Stop draining

**Stop Drain Timeout (ms)** defaults to **5000**, with a range of **0–30000**. After the subscription closes and active triggers finish, the processor tries to commit buffered records within this additional time. Zero disables the attempt. Downstream backpressure, loss of primary ownership, interruption or a repository failure ends draining early. Remaining records produce a warning and stay in memory for Start.

A pending commit keeps its reservation until NiFi reports success or failure; a timeout does not make it safe to retry the same batch. The setting bounds the extra drain wait, not an already-active trigger, blocked repository I/O, or total NiFi shutdown time. See [operations](operations.md) for the lifecycle details.

## ETL output options

See [output](output.md) for field names, framing, timestamp precision and routing semantics. Output projection happens during encoding, so omitted metadata is not computed. Defaults retain the existing schema and delivery route. `dataType` and `engineeringUnits` are optional server metadata snapshots, read only during connection setup. They remain null when unavailable; they are not required to subscribe to values.

For compact line-oriented ingestion, set Output Fields to `tag,value,statusCode,sourceTimestamp`, Output Format to `NDJSON`, and choose the timestamp representation your destination expects. A small positive Maximum Batch Delay can reduce the number of small FlowFiles. Configure NiFi's Run Schedule to match the desired latency; the batching deadline cannot schedule a paused processor.

Enabling Route by Quality exposes `bad` and `uncertain`; connect or explicitly terminate them. Default NiFi backpressure can pause scheduling when a connected route is full. Records retain their original output and routing after Stop/Start, so keep old route connections until their buffers drain.
