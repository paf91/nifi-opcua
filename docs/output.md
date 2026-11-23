# Output and delivery guarantees

By default, each success FlowFile contains a JSON array. Each notification includes `tag`, `nodeId`, `value`, `valueType`, `statusCode` (hex), `quality`, `sourceTimestamp`, and `receivedTimestamp`. For URI-configured tags only, `configuredNodeId` and `namespaceUri` are also included; `nodeId` remains the numeric address used in that session. Unavailable timestamps are null. Endpoint and security settings are FlowFile attributes; provenance records the source endpoint.

## Output selection and framing

**Output Fields** is a comma-separated list. The default retains the fields above. Include `value` and at least one identity (`tag` or `nodeId`); unknown, duplicate and empty field names fail validation. Omitted fields are not formatted or written. A compact selection is:

```text
tag,value,statusCode,sourceTimestamp
```

```json
{"tag":"Temperature","value":42.5,"statusCode":"0x00000000","sourceTimestamp":"2026-10-29T12:00:00Z"}
```

Retaining `statusCode` is recommended: `quality=Good` alone does not reveal overflow and other information bits. Projection reduces serialization, retained JSON bytes and downstream traffic; it does not reduce OPC UA sampling or incoming subscription traffic. Use sampling intervals and server-side deadbands for that purpose.

**Output Format** selects `JSON Array` (default) or `NDJSON`. NDJSON writes one complete JSON object followed by a newline, including the last record, with MIME type `application/x-ndjson`. Embedded newlines in strings remain escaped. Record and FlowFile byte limits include UTF-8 content and the selected framing. The conservative validation rule `Maximum Record Bytes + 2 <= Maximum FlowFile Bytes` applies to both formats.

**Timestamp Format** selects `ISO-8601` (default) or numeric `Epoch Milliseconds` for `sourceTimestamp`, optional `serverTimestamp`, and `receivedTimestamp`. Epoch milliseconds discard sub-millisecond precision; ISO retains the precision supplied in the OPC UA DateTime (100 ns units). Separate OPC UA picosecond fields are not emitted. A DateTime in the `value` field retains its ISO representation in either mode. Missing/zero protocol timestamps remain null, never epoch zero. Pre-1970 times are converted correctly.

## Quality routing and optional batching

With **Route by Quality = false** (default), all readings use `success`. When enabled, Good uses `success`, Bad uses `bad`, and Uncertain or missing status uses `uncertain`. Connect or explicitly terminate the extra relationships. Routing uses the original status even when `quality` and `statusCode` are omitted. Each FlowFile contains a contiguous run of the same route; records are not reordered to fill larger batches. A status with Good severity and an overflow bit still uses `success`, retaining that bit if `statusCode` is selected.

**Maximum Batch Delay (ms)** defaults to zero. A positive value allows small bursts to accumulate until the oldest queued record reaches that age. Count/byte limits, a route boundary, buffer pressure, Stop and draining retained data can flush earlier. The delay is not an end-to-end latency guarantee: NiFi scheduling and downstream backpressure can postpone a trigger. Active triggers wait at most 50 ms for a pending commit or a partial batch, releasing the buffer monitor while waiting. They do not request NiFi's full Yield Duration just because a commit is pending. No extra scheduling threads or waits are added to OPC UA callbacks.

## Optional cached tag metadata

Selecting `dataType` reads the variable's OPC UA DataType attribute. Selecting `engineeringUnits` resolves its standard `0:EngineeringUnits` property and reads the EUInformation value. Only selected metadata is requested. Values are snapshots, not monitored metadata changes: they refresh when a new session/subscription is established. Stop/Start can refresh them explicitly. An existing-session transport reconnect does not force a metadata refresh.

Optional metadata reads and browse-path translations start with at most 100 operations per request. On `Bad_TooManyOperations`, `Bad_RequestTooLarge` or `Bad_ResponseTooLarge`, the processor halves the number of operations actually sent. Splitting stops at one operation. All optional metadata requests together have one **Request Timeout (ms)** budget. Cancellation stops the connection attempt. Missing, unreadable, malformed, ambiguous or remote properties become null. A service error or timeout can leave metadata unavailable while value subscription proceeds. Diagnostics include missing-field counts and the exception type, with the symbolic OPC UA status when it is known.

Cached metadata strings are limited to 1024 characters each. Oversized metadata is unavailable. The lookup of `EngineeringUnits` accepts property NodeIds with string identifiers up to 4096 Unicode code points or opaque identifiers up to 4096 bytes. Only standard EUInformation encodings are decoded. Binary bodies larger than 16 KiB, or XML/JSON bodies longer than 16384 characters, are not decoded. Normal record and buffer byte limits still apply when the cached metadata is emitted. `engineeringUnits` contains `namespaceUri`, `unitId`, `displayName` and `description`; the text fields contain the server's returned localized text without its locale. Custom `dataType` namespace indexes belong to the originating server session.

For example, select `tag,value,sourceTimestamp,statusCode,dataType,engineeringUnits` when downstream consumers need self-contained records. For large streams, emitting a separate tag catalogue downstream can avoid repeating stable metadata. Use NiFi record processors for business calculations, enrichment and column renaming.

The processor encodes the typed value returned by the server. Booleans, ordinary numbers, strings and arrays retain their JSON types. Both adapters emit multidimensional arrays as nested JSON arrays. The modern adapter streams Milo's flat matrix representation without first constructing nested arrays. Legacy Milo already supplies nested arrays. Int64/UInt64 are strings to avoid precision loss; ByteStrings use Base64. Bad/Uncertain values are emitted with quality information so downstream flow logic can decide how to handle them.

Starting in **1.0.3**, an opaque `ExtensionObject` uses one of the following representations. Its body is streamed through the record byte limit without automatic decoding or display-string expansion. XML and JSON bodies are quoted strings, not parsed or embedded as executable/raw JSON. Null bodies remain JSON null.

| Body | `value` representation |
|---|---|
| OPC UA Binary | `{"encoding":"opcua-binary","encodingId":"ns=2;i=999","body":"AQID"}` (Base64 body) |
| OPC UA XML | `{"encoding":"opcua-xml","encodingId":"ns=2;i=999","body":"<custom/>"}` |
| OPC UA JSON (Milo 1) | `{"encoding":"opcua-json","typeId":"ns=2;i=999","body":"{\"custom\":1}"}` |

Other unsupported Java/Milo objects use `{"encoding":"unsupported","type":"fully.qualified.ClassName"}`. Their value is omitted rather than invoking an arbitrary `toString()` that could allocate or block. UUID and StatusCode values retain their bounded `encoding: display`, `type`, and `text` representation. Consumers of the previous ExtensionObject/unsupported `text` fallback must migrate to the encoded body or explicitly handle the unsupported marker. This is a representation change, not a decoded schema for proprietary structures.

This is **data-change subscription**, not a periodic full snapshot, event/alarm subscription, historical read, or write client. Normally the server sends initial values and later changes; an unchanged value need not produce repeated FlowFiles.

Notifications wait in a bounded in-memory queue. A batch stays reserved until NiFi's asynchronous commit succeeds; a failed commit releases it for retry. A second trigger cannot claim the same in-flight batch. Count or byte capacity exhaustion drops **newest** notifications and reports a bulletin/counter. Full buffers reject before encoding; partial byte capacity limits the encoder to the remaining space. These drops count as overflow; oversized-record counts apply when the record limit itself is the binding limit. Final admission rechecks both capacities atomically. Server notification-loss callbacks also produce a bulletin/counter. Callbacks never block waiting for NiFi storage.

These byte limits constrain processor encoding and retained JSON. They do not bound allocations made by the OPC UA SDK before it passes decoded values to the processor callback.

Both Milo clients advertise a 2 MiB maximum for incoming UA TCP messages by default. This limit applies to the encoded incoming message. Decoded values can occupy more heap memory than their wire representation. The processor's record, buffer and FlowFile limits apply to serialized JSON and are independent of this advertised limit.

Discovery uses explicit clients with bounded connect, handshake and GetEndpoints operations. Request Timeout applies per operation, not to the entire multi-step startup. Stop cancels the active attempt, including synchronous monitored-item creation. Each connection owns one transport event-loop thread; cleanup closes that group to abort incomplete handshakes. Orderly disconnect waits at most one second before transport shutdown.

Milo handles transport reconnects. Failed transfers, bad subscription status and watchdog expiry request subscription recreation on a later NiFi trigger, with a retry delay. Recovery is consequently delayed while NiFi scheduling is paused/backpressured. The processor closes sessions and PKI watchers when stopped and does not shut down Milo resources shared by other processors.

Stop first closes ingress, then attempts to commit already buffered notifications within **Stop Drain Timeout (ms)**, default **5000**; zero disables this attempt. The drain uses the normal batch count/byte limits and requires the batch's destination relationship to be available and the node to retain ownership. Backpressure, failure, interruption or deadline expiry ends the attempt. A warning reports the remaining count and reason only if records are still uncommitted. A submitted asynchronous commit can finish after that warning; its reservation is never released merely because waiting timed out.

Any remaining records are retained in memory. On Start they are drained before opening a new subscription, even if the server is unavailable or properties have changed. Retained records keep the old endpoint and security policy/mode in FlowFile attributes, and the old endpoint in provenance; new settings and buffer capacity apply after draining. The buffer also retains its original field selection, framing, timestamp representation, quality routing, and maximum record/FlowFile byte limits until drained. Output attributes describe those retained settings. The current batch count ceiling still applies. Old quality relationships remain exposed while records need them; do not remove their connections before draining. Pending asynchronous commits retain their reservation across Stop/Start. The memory is released as batches commit or when the processor is removed. The buffer stays on its original node during primary failover and can be drained when that node becomes primary again; it is not replicated.

There is **no exactly-once or outage-lossless guarantee**: server queue overflow, unrecoverable republish gaps, crashes, stop/start, and cluster failover can lose values or produce repeats. Buffered, uncommitted notifications survive processor Stop/Start in the same JVM, but are not durable across a NiFi/JVM restart or crash. FlowFiles already committed to NiFi use its normal repository persistence. Changes occurring while the subscription is stopped are not recovered from OPC history. Applications needing historical recovery must add server historization and a separate catch-up design.
Periodic health reports use a separate `diagnostics` relationship and `opcua.type=diagnostics`, with one JSON object per FlowFile. They are disabled by default and automatically terminated unless routed. The measurement FlowFile byte limit applies to arrays and NDJSON, not these bounded metadata reports. See [diagnostics](diagnostics.md).

## Example

```json
[
  {
    "tag": "DemoSwitch",
    "nodeId": "ns=2;s=Demo.Boolean",
    "value": true,
    "valueType": "Boolean",
    "statusCode": "0x00000000",
    "quality": "Good",
    "sourceTimestamp": "2026-01-01T12:00:00Z",
    "receivedTimestamp": "2026-01-01T12:00:00.125Z"
  }
]
```

| Field | Meaning |
|---|---|
| tag | Configured alias, or the NodeId when no alias is supplied |
| nodeId | Numeric namespace NodeId actually used in the originating session |
| configuredNodeId / namespaceUri | Original URI address and resolved URI; present only for URI-configured tags |
| value | Typed JSON value, null, or documented fallback |
| valueType | Java/Milo value class simple name; null when no value is supplied |
| statusCode | Eight-digit hexadecimal OPC UA status; null if unavailable |
| quality | Good, Uncertain, Bad, or Unknown |
| sourceTimestamp | UTC source time; null if absent or zero |
| receivedTimestamp | Local encoding time, ISO or epoch milliseconds |
| serverTimestamp | Optional server timestamp, ISO or epoch milliseconds; null if absent/zero |
| dataType | Optional cached OPC UA DataType NodeId; null if unavailable |
| engineeringUnits | Optional cached EUInformation object; null if unavailable |

`valueType` describes the runtime representation; it is not a stable OPC UA DataType NodeId. Array type names are Java class names such as `Float[]`. Int64/UInt64, non-finite floating-point values and some complex values need downstream handling. Receipt time measures local observation, not event ordering or exactly-once delivery.

Attributes: `mime.type`, `record.count`, `opcua.endpoint`, `opcua.security.policy`, `opcua.security.mode`, `opcua.output.fields`, `opcua.output.format`, and `opcua.timestamp.format`. `opcua.quality.route` is included when quality routing is enabled. `opcua.output.fields` lists the selected fields in canonical order; URI-only fields are still absent for numeric tags. Security attributes reflect the selected channel settings (for example, `None` / `None` or `Basic256Sha256` / `SignAndEncrypt`), not the username-token encryption policy. Provenance records a RECEIVE event with the endpoint. Old buffered batches retain their original endpoint and channel security attributes after a configuration change.

## Lifecycle summary

| Event | Result |
|---|---|
| Processor Stop → Start, same JVM | Pending notifications drain before a new subscription opens |
| Properties edited while stopped | Old records drain with their original output, routing, endpoint and security settings, then new settings apply |
| JVM restart, crash, processor removal | Uncommitted memory buffer is lost |
| Backpressure | NiFi stops triggering; callbacks can fill the bounded buffer |
| Full local buffer | Newest notifications are dropped; uncommitted batches are preserved |
| Primary role revoked | Startup is cancelled, connection closed, old callbacks fenced |
| Another node becomes primary | Fresh subscription; old node's buffer is not transferred |
| Server restart | Milo reconnects; failed transfer/watchdog can request resubscription |
| Subscription stopped for a period | Intermediate changes are not recovered from history |

No historical reads, writes, event/alarm subscriptions, automatic node discovery, deduplication or durable local journal are implemented. For consumers that need ordered or lossless data, design explicit timestamp handling, deduplication and server-side history recovery.
