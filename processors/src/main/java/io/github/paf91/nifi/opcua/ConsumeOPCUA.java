/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.*;
import org.apache.nifi.annotation.lifecycle.*;
import org.apache.nifi.annotation.notification.*;
import org.apache.nifi.components.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.milo.opcua.stack.core.UaExceptionStatus;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;

import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.*;

@Tags({"opcua", "opc", "subscription", "industrial", "iot"})
@CapabilityDescription(
        "Subscribes to OPC UA Value changes with configurable security and anonymous or"
            + " username/password authentication. Defaults to Basic256Sha256 / SignAndEncrypt."
            + " Paste a list of NodeIds into Tags, or add individual tag properties. Outputs JSON"
            + " arrays or NDJSON with selectable fields. Full server certificate validation is"
            + " enabled by default. Buffers are in memory; overflow drops newest notifications and"
            + " raises a bulletin.")
@WritesAttributes({
    @WritesAttribute(
            attribute = "mime.type",
            description =
                    "Content type of the measurement batch: application/json or"
                            + " application/x-ndjson. Diagnostics FlowFiles use application/json."),
    @WritesAttribute(
            attribute = "record.count",
            description = "Number of records in a measurement FlowFile."),
    @WritesAttribute(
            attribute = "opcua.endpoint",
            description = "Endpoint recorded for the measurement buffer, including retained data."),
    @WritesAttribute(
            attribute = "opcua.security.policy",
            description =
                    "Channel security policy recorded for the measurement buffer, including"
                            + " retained data."),
    @WritesAttribute(
            attribute = "opcua.security.mode",
            description =
                    "Channel security mode recorded for the measurement buffer, including retained"
                            + " data."),
    @WritesAttribute(
            attribute = "opcua.output.fields",
            description =
                    "Selected measurement output fields, comma-separated in canonical order."),
    @WritesAttribute(
            attribute = "opcua.output.format",
            description = "Format used to encode the measurement batch: JSON Array or NDJSON."),
    @WritesAttribute(
            attribute = "opcua.timestamp.format",
            description =
                    "Timestamp format used to encode the measurement batch: ISO-8601 or Epoch"
                            + " Milliseconds."),
    @WritesAttribute(
            attribute = "opcua.quality.route",
            description =
                    "Measurement relationship name, set when Route by Quality was enabled for the"
                            + " batch."),
    @WritesAttribute(
            attribute = "opcua.type",
            description = "Set to diagnostics on diagnostics FlowFiles only.")
})
@DynamicProperty(
        name = "Tag alias",
        value =
                "NodeId (ns= or nsu=) or JSON with nodeId, samplingIntervalMs, queueSize,"
                        + " discardOldest, deadbandType, deadbandValue",
        description =
                "Add one property per monitored tag using the + button. Example:"
                        + " PumpRunning = ns=2;s=Example.Pump.Running. JSON options override the"
                        + " processor defaults.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@PrimaryNodeOnly
@DefaultSchedule(period = "100 ms")
@Restricted(
        restrictions = {
            @Restriction(
                    requiredPermission = RequiredPermission.READ_FILESYSTEM,
                    explanation = "Reads client certificates and private keys."),
            @Restriction(
                    requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                    explanation = "Initializes and maintains the OPC UA PKI trust directory.")
        })
public class ConsumeOPCUA extends AbstractSessionFactoryProcessor implements VerifiableProcessor {
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static PropertyDescriptor text(
            String name, String description, boolean sensitive, boolean required) {
        return new PropertyDescriptor.Builder()
                .name(name)
                .description(description)
                .required(required)
                .sensitive(sensitive)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    private static PropertyDescriptor number(
            String name, String description, String defaultValue, int max) {
        return number(name, description, defaultValue, max, 1);
    }

    private static PropertyDescriptor number(
            String name, String description, String defaultValue, int max, int minimum) {
        return new PropertyDescriptor.Builder()
                .name(name)
                .description(description)
                .required(true)
                .defaultValue(defaultValue)
                .addValidator(
                        (subject, input, context) -> {
                            boolean valid;
                            try {
                                int n = Integer.parseInt(input);
                                valid = n >= minimum && n <= max;
                            } catch (Exception e) {
                                valid = false;
                            }
                            return new ValidationResult.Builder()
                                    .subject(subject)
                                    .valid(valid)
                                    .explanation(
                                            "Must be an integer between " + minimum + " and " + max)
                                    .build();
                        })
                .build();
    }

    public static final PropertyDescriptor ENDPOINT =
            text(
                    "Endpoint URL",
                    "OPC TCP discovery URL, e.g. opc.tcp://server:4840. The server must offer"
                            + " the configured security policy, message mode and authentication.",
                    false,
                    true);
    public static final PropertyDescriptor SECURITY_POLICY =
            new PropertyDescriptor.Builder()
                    .name("Security Policy")
                    .description(
                            "OPC UA channel security policy. None disables channel protection;"
                                + " Basic128Rsa15 and Basic256 are legacy compatibility options."
                                + " Selection is exact; security is never automatically"
                                + " downgraded.")
                    .required(true)
                    .allowableValues(
                            "None",
                            "Basic128Rsa15",
                            "Basic256",
                            "Basic256Sha256",
                            "Aes128_Sha256_RsaOaep",
                            "Aes256_Sha256_RsaPss")
                    .defaultValue("Basic256Sha256")
                    .build();
    public static final PropertyDescriptor SECURITY_MODE =
            new PropertyDescriptor.Builder()
                    .name("Security Mode")
                    .description(
                            "None for Security Policy None; otherwise Sign or SignAndEncrypt. Sign"
                                    + " authenticates messages without encrypting their contents.")
                    .required(true)
                    .allowableValues("None", "Sign", "SignAndEncrypt")
                    .defaultValue("SignAndEncrypt")
                    .build();
    public static final PropertyDescriptor AUTHENTICATION =
            new PropertyDescriptor.Builder()
                    .name("Authentication")
                    .description(
                            "OPC UA session identity. Anonymous requires no username or password.")
                    .required(true)
                    .allowableValues(OpcSecurity.USERNAME, OpcSecurity.ANONYMOUS)
                    .defaultValue(OpcSecurity.USERNAME)
                    .build();
    public static final PropertyDescriptor ALLOW_UNENCRYPTED_PASSWORD =
            new PropertyDescriptor.Builder()
                    .name("Allow Unencrypted Password")
                    .description(
                            "Controls whether the password may be sent without encryption on the"
                                + " OPC UA connection. The password can be encrypted separately in"
                                + " the username token or protected by a SignAndEncrypt channel."
                                + " None channels only permits an unencrypted password when"
                                + " Security Mode is None. Never requires token encryption or"
                                + " SignAndEncrypt. Always also permits unencrypted passwords on"
                                + " Sign channels. Supported encrypted tokens are always preferred."
                                + " Does not affect Anonymous authentication.")
                    .required(true)
                    .allowableValues(
                            new AllowableValue(
                                    "None channels only",
                                    "None channels only",
                                    "Allow an unencrypted password only when Security Mode is"
                                            + " None."),
                            new AllowableValue(
                                    "Never",
                                    "Never",
                                    "Require username-token encryption or SignAndEncrypt."),
                            new AllowableValue(
                                    "Always",
                                    "Always (insecure)",
                                    "Also allow an unencrypted password on Sign channels."))
                    .defaultValue(UnencryptedPasswordPolicy.NONE_CHANNELS_ONLY.value())
                    .build();
    public static final PropertyDescriptor APPLICATION_URI =
            text(
                    "Application URI",
                    "Optional advanced override. Leave unset to read the client application"
                        + " identity from the URI subject alternative name in your client"
                        + " certificate for secured channels. Without channel security, defaults to"
                        + " "
                            + OpcSecurity.DEFAULT_APPLICATION_URI
                            + ". This is not the server URL. For secured channels an override must"
                            + " exactly match the certificate.",
                    false,
                    false);
    public static final PropertyDescriptor SERVER_CERTIFICATE_VALIDATION =
            new PropertyDescriptor.Builder()
                    .name("Server Certificate Validation")
                    .description(
                            "Full verifies server certificate trust, validity, usage, revocation,"
                                    + " Application URI and endpoint hostname/IP. Certificate skips"
                                    + " only the hostname/IP match. None bypasses the server"
                                    + " certificate validator and permits server impersonation."
                                    + " Applies also to encrypted username tokens on an unsecured"
                                    + " channel. Does not change channel security, client identity"
                                    + " requirements or user authentication.")
                    .required(true)
                    .allowableValues(
                            new AllowableValue(
                                    "Full", "Full", "All server certificate checks (recommended)."),
                            new AllowableValue(
                                    "Certificate",
                                    "Certificate",
                                    "All checks except the server hostname/IP match."),
                            new AllowableValue(
                                    "None",
                                    "None (insecure)",
                                    "Skip server certificate validation; server impersonation is"
                                            + " possible."))
                    .defaultValue("Full")
                    .build();
    public static final PropertyDescriptor CLIENT_IDENTITY_FORMAT =
            new PropertyDescriptor.Builder()
                    .name("Client Identity Format")
                    .description(
                            "PEM/DER uses separate certificate and private-key files. JKS or PKCS12"
                                    + " uses Client Keystore File and an optional Client Key Alias."
                                    + " Client identity is unused with channel Security Mode None.")
                    .required(true)
                    .allowableValues("PEM/DER", "JKS", "PKCS12")
                    .defaultValue("PEM/DER")
                    .build();
    public static final PropertyDescriptor CERTIFICATE =
            text(
                    "Client Certificate File",
                    "Absolute path to X.509 PEM chain (leaf first), or DER leaf certificate, on the"
                        + " NiFi host. Required for Sign and SignAndEncrypt with Client Identity"
                        + " Format PEM/DER; unused with None.",
                    false,
                    false);
    public static final PropertyDescriptor PRIVATE_KEY =
            text(
                    "Client Private Key File",
                    "Absolute path to RSA PEM private key (PKCS#1/PKCS#8, optional encryption) or"
                            + " unencrypted PKCS#8 DER. Required for Sign and SignAndEncrypt with"
                            + " Client Identity Format PEM/DER; unused with None.",
                    false,
                    false);
    public static final PropertyDescriptor KEYSTORE =
            text(
                    "Client Keystore File",
                    "Absolute path to the JKS or PKCS12 client identity store on each NiFi host."
                            + " Use instead of Client Certificate File and Client Private Key File."
                            + " Loaded when a new connection is created; Stop/Start applies file"
                            + " changes.",
                    false,
                    false);
    public static final PropertyDescriptor KEYSTORE_PASSWORD =
            text(
                    "Client Keystore Password",
                    "Optional store password. Leave unset for an empty-password store or a PKCS12"
                            + " store without integrity protection. Does not supply Private Key"
                            + " Password.",
                    true,
                    false);
    public static final PropertyDescriptor KEY_ALIAS =
            text(
                    "Client Key Alias",
                    "Optional alias of a private-key entry in the client keystore. Leave unset only"
                            + " when the store contains exactly one private-key entry.",
                    false,
                    false);
    public static final PropertyDescriptor KEY_PASSWORD =
            text(
                    "Private Key Password",
                    "Password for the PEM private key or selected keystore private-key entry. Leave"
                        + " unset for an unencrypted PEM key or an empty key password. Independent"
                        + " of Client Keystore Password; set both when they use the same password.",
                    true,
                    false);
    public static final PropertyDescriptor SERVER_TRUST_SOURCE =
            new PropertyDescriptor.Builder()
                    .name("Server Trust Source")
                    .description(
                            "PKI Directory uses the existing directory trust list. JKS or PKCS12"
                                + " uses only trusted-certificate entries in Server Truststore"
                                + " File. An optional PKI Directory then supplies issuers and CRLs,"
                                + " while its trusted/certs entries are ignored. Does not change"
                                + " certificate validation rules. Unused when Server Certificate"
                                + " Validation is None.")
                    .required(true)
                    .allowableValues("PKI Directory", "JKS", "PKCS12")
                    .defaultValue("PKI Directory")
                    .build();
    public static final PropertyDescriptor TRUSTSTORE =
            text(
                    "Server Truststore File",
                    "Absolute path to a JKS or PKCS12 store containing trusted X.509 certificate"
                        + " entries. Private-key entries do not establish trust. Loaded when a new"
                        + " connection is created; Stop/Start applies file changes.",
                    false,
                    false);
    public static final PropertyDescriptor TRUSTSTORE_PASSWORD =
            text(
                    "Server Truststore Password",
                    "Optional truststore password. Leave unset for an empty-password store or a"
                            + " PKCS12 store without integrity protection. A wrong password is not"
                            + " ignored.",
                    true,
                    false);
    public static final PropertyDescriptor PKI =
            text(
                    "PKI Directory",
                    "Server trust folder on each NiFi host (absolute path). With Server Trust"
                        + " Source PKI Directory, trusted/certs supplies trusted certificates. With"
                        + " a JKS or PKCS12 truststore, this optional directory supplies only"
                        + " issuer certificates and CRLs; trusted/certs is ignored. Required when"
                        + " the selected endpoint needs certificate validation and Server Trust"
                        + " Source is PKI Directory. Unused when validation is None. No"
                        + " certificates are automatically trusted.",
                    false,
                    false);
    public static final PropertyDescriptor TAG_LIST =
            new PropertyDescriptor.Builder()
                    .name("Tags")
                    .description(
                            "Paste one complete NodeId per line, or alias | NodeId. Example:"
                                + " Temperature | ns=2;s=Example.Tank.Temperature. Blank lines and"
                                + " lines starting with # are ignored. Alternatively paste a JSON"
                                + " array of objects with nodeId, optional alias,"
                                + " samplingIntervalMs, queueSize, discardOldest, deadbandType and"
                                + " deadbandValue. NodeIds support numeric ns= indexes and nsu="
                                + " namespace URIs. Entries use processor defaults unless"
                                + " overridden. Individual tag properties are merged; duplicate"
                                + " aliases or NodeIds are rejected. Stop, edit and restart to"
                                + " apply changes.")
                    .required(false)
                    .addValidator(
                            (subject, input, context) ->
                                    new ValidationResult.Builder()
                                            .subject(subject)
                                            .valid(true)
                                            .build())
                    .build();
    public static final PropertyDescriptor USERNAME =
            text("Username", "Required for Username/Password authentication.", false, false);
    public static final PropertyDescriptor PASSWORD =
            text(
                    "Password",
                    "Required for Username/Password authentication. Use a sensitive NiFi"
                            + " parameter.",
                    true,
                    false);
    public static final PropertyDescriptor PUBLISHING =
            number(
                    "Publishing Interval (ms)",
                    "Requested subscription publishing interval; server may revise it.",
                    "1000",
                    86400000);
    public static final PropertyDescriptor SAMPLING =
            number(
                    "Default Sampling Interval (ms)",
                    "Default requested sampling interval. Per-tag JSON can override it, including 0"
                            + " for fastest supported sampling.",
                    "1000",
                    86400000);
    public static final PropertyDescriptor SERVER_QUEUE =
            number(
                    "Default Monitored Item Queue Size",
                    "Requested per-tag queue on the OPC server; server may revise it. This is"
                            + " independent of the local buffer.",
                    "100",
                    1000000);
    public static final PropertyDescriptor BUFFER =
            number(
                    "Notification Buffer Capacity",
                    "Maximum notifications in memory. Drop NEWEST on overflow to preserve"
                            + " uncommitted batches; report drops in logs and counters.",
                    "10000",
                    1000000);
    public static final PropertyDescriptor BATCH =
            number(
                    "Max Records Per FlowFile",
                    "Maximum notifications per FlowFile. A ceiling, not a minimum batch size.",
                    "500",
                    100000);
    public static final PropertyDescriptor BUFFER_BYTES =
            number(
                    "Notification Buffer Bytes",
                    "Maximum serialized notification bytes retained in memory, including pending"
                            + " commits. Both count and byte limits apply; newest notifications are"
                            + " dropped on overflow. Does not bound total JVM or Milo memory.",
                    "67108864",
                    Integer.MAX_VALUE);
    public static final PropertyDescriptor RECORD_BYTES =
            number(
                    "Maximum Record Bytes",
                    "Maximum UTF-8 JSON bytes per notification. Oversized notifications are"
                            + " rejected during serialization and counted separately. Must fit the"
                            + " buffer and the FlowFile byte limit.",
                    "1048576",
                    67108864);
    public static final PropertyDescriptor BATCH_BYTES =
            number(
                    "Maximum FlowFile Bytes",
                    "Maximum output bytes including JSON array framing or NDJSON newlines. Must be"
                            + " at least Maximum Record Bytes plus two. Retained batches use their"
                            + " original limits after Stop/Start.",
                    "4194304",
                    67108866);
    public static final PropertyDescriptor OUTPUT_FIELDS =
            new PropertyDescriptor.Builder()
                    .name("Output Fields")
                    .description(
                            "Comma-separated fields:"
                                + " tag,nodeId,configuredNodeId,namespaceUri,value,valueType,statusCode,quality,sourceTimestamp,receivedTimestamp,serverTimestamp,dataType,engineeringUnits."
                                + " Include value and tag or nodeId. Compact example:"
                                + " tag,value,statusCode,sourceTimestamp. Omitted fields are not"
                                + " computed. dataType and engineeringUnits request cached server"
                                + " metadata; unavailable metadata is null.")
                    .required(true)
                    .defaultValue(OutputSettings.DEFAULT_FIELDS)
                    .addValidator(
                            (subject, input, context) -> {
                                try {
                                    OutputSettings.parseFields(input);
                                    return new ValidationResult.Builder()
                                            .subject(subject)
                                            .valid(true)
                                            .build();
                                } catch (IllegalArgumentException e) {
                                    return new ValidationResult.Builder()
                                            .subject(subject)
                                            .valid(false)
                                            .explanation(e.getMessage())
                                            .build();
                                }
                            })
                    .build();
    public static final PropertyDescriptor OUTPUT_FORMAT =
            new PropertyDescriptor.Builder()
                    .name("Output Format")
                    .description(
                            "JSON array or one JSON object per line (NDJSON). Retained records keep"
                                    + " their original output settings after Stop/Start.")
                    .required(true)
                    .allowableValues("JSON Array", "NDJSON")
                    .defaultValue("JSON Array")
                    .build();
    public static final PropertyDescriptor TIMESTAMP_FORMAT =
            new PropertyDescriptor.Builder()
                    .name("Timestamp Format")
                    .description(
                            "Format of sourceTimestamp, serverTimestamp and receivedTimestamp."
                                + " Epoch Milliseconds discards sub-millisecond precision. Does not"
                                + " change OPC UA DateTime values in the value field.")
                    .required(true)
                    .allowableValues("ISO-8601", "Epoch Milliseconds")
                    .defaultValue("ISO-8601")
                    .build();
    public static final PropertyDescriptor QUALITY_ROUTING =
            new PropertyDescriptor.Builder()
                    .name("Route by Quality")
                    .description(
                            "When true, send Bad readings to bad and Uncertain or missing status to"
                                    + " uncertain; Good readings use success. Connect or explicitly"
                                    + " terminate the additional relationships. Routing uses the"
                                    + " original status even if statusCode/quality are omitted from"
                                    + " output. Records are not reordered across routes.")
                    .required(true)
                    .allowableValues("true", "false")
                    .defaultValue("false")
                    .build();
    public static final PropertyDescriptor BATCH_DELAY =
            number(
                    "Maximum Batch Delay (ms)",
                    "Optional accumulation interval measured from the oldest queued record. Zero"
                        + " disables the delay. Count/byte limits, route changes, buffer pressure"
                        + " and Stop can make a batch ready earlier. Scheduling and backpressure"
                        + " can delay delivery beyond this interval.",
                    "0",
                    60000,
                    0);
    public static final PropertyDescriptor DIAGNOSTICS_INTERVAL =
            number(
                    "Diagnostics Interval (seconds)",
                    "Emit a bounded JSON health report to diagnostics at this interval; 0 disables"
                            + " reports. Reports are generated on trigger, so NiFi backpressure can"
                            + " delay them. Reports contain metadata, never sensor values or"
                            + " passwords.",
                    "0",
                    86400,
                    0);
    public static final PropertyDescriptor TIMEOUT =
            number(
                    "Request Timeout (ms)",
                    "Timeout per OPC operation, including discovery connect and GetEndpoints."
                        + " Optional tag metadata requests share one timeout budget. Startup has"
                        + " multiple operations; item creation is chunked in batches of up to 100,"
                        + " bounded by the server's advertised limit. Stop cancels startup. Cleanup"
                        + " waits at most 1 second per disconnect.",
                    "10000",
                    300000);
    public static final PropertyDescriptor RETRY =
            number(
                    "Reconnect Delay (ms)",
                    "Delay between attempts to recreate a failed subscription. Milo handles normal"
                            + " transport reconnects automatically.",
                    "5000",
                    300000);
    public static final PropertyDescriptor STOP_DRAIN_TIMEOUT =
            number(
                    "Stop Drain Timeout (ms)",
                    "Maximum additional wait to commit buffered notifications after active tasks"
                        + " finish and the OPC UA connection is closed. Zero disables draining."
                        + " Backpressure or failure ends the attempt; remaining records are kept"
                        + " for Start. Does not bound an already-running NiFi repository"
                        + " operation.",
                    "5000",
                    30000,
                    0);
    public static final Relationship SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description(
                            "Data changes. Includes all qualities unless Route by Quality is"
                                    + " enabled.")
                    .build();
    public static final Relationship BAD =
            new Relationship.Builder()
                    .name("bad")
                    .description("Bad OPC UA status when Route by Quality is enabled.")
                    .build();
    public static final Relationship UNCERTAIN =
            new Relationship.Builder()
                    .name("uncertain")
                    .description(
                            "Uncertain or missing OPC UA status when Route by Quality is enabled.")
                    .build();
    public static final Relationship DIAGNOSTICS =
            new Relationship.Builder()
                    .name("diagnostics")
                    .description(
                            "Optional JSON connection, namespace, negotiated settings and buffer"
                                    + " health reports. Automatically terminated by default.")
                    .autoTerminateDefault(true)
                    .build();
    private static final List<PropertyDescriptor> PROPERTIES =
            Arrays.asList(
                    ENDPOINT,
                    SECURITY_POLICY,
                    SECURITY_MODE,
                    AUTHENTICATION,
                    ALLOW_UNENCRYPTED_PASSWORD,
                    SERVER_CERTIFICATE_VALIDATION,
                    TAG_LIST,
                    CLIENT_IDENTITY_FORMAT,
                    CERTIFICATE,
                    PRIVATE_KEY,
                    KEYSTORE,
                    KEYSTORE_PASSWORD,
                    KEY_ALIAS,
                    KEY_PASSWORD,
                    SERVER_TRUST_SOURCE,
                    TRUSTSTORE,
                    TRUSTSTORE_PASSWORD,
                    PKI,
                    USERNAME,
                    PASSWORD,
                    APPLICATION_URI,
                    PUBLISHING,
                    SAMPLING,
                    SERVER_QUEUE,
                    BUFFER,
                    BUFFER_BYTES,
                    RECORD_BYTES,
                    BATCH,
                    BATCH_BYTES,
                    OUTPUT_FIELDS,
                    OUTPUT_FORMAT,
                    TIMESTAMP_FORMAT,
                    QUALITY_ROUTING,
                    BATCH_DELAY,
                    DIAGNOSTICS_INTERVAL,
                    TIMEOUT,
                    RETRY,
                    STOP_DRAIN_TIMEOUT);
    private volatile NotificationBuffer buffer;
    private volatile boolean qualityRouting;
    private static final long ACTIVE_WAIT_NANOS =
            java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(50);
    private volatile AutoCloseable connection;
    private final Object lifecycle = new Object();
    // Transport ownership must not wait for ProcessSession repository I/O.
    private final Object connectionLock = new Object();
    private volatile boolean running;
    private volatile ProcessContext scheduledContext;
    private volatile ProcessSessionFactory sessionFactory;
    private final Object drainLock = new Object();
    private StopDrain stopDrain;
    private volatile ConnectionAttempt attempt;
    private String bufferEndpoint;
    private String bufferSecurityPolicy;
    private String bufferSecurityMode;
    private long bufferBatchBytes;
    private boolean replaceBufferWhenDrained;
    private final AtomicBoolean recovery = new AtomicBoolean();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong dataLost = new AtomicLong();
    private final AtomicLong oversized = new AtomicLong();
    private final AtomicLong encodingErrors = new AtomicLong();
    private final AtomicLong totalDropped = new AtomicLong();
    private final AtomicLong totalDataLost = new AtomicLong();
    private final AtomicLong connectionAttempts = new AtomicLong();
    private final AtomicLong successfulConnections = new AtomicLong();
    private volatile ConnectionDiagnostics diagnostics = new ConnectionDiagnostics();
    private String lastFailure;
    private final AtomicBoolean diagnosticsPending = new AtomicBoolean();

    private static final class DiagnosticSchedule {
        volatile long next;
    }

    private volatile DiagnosticSchedule diagnosticSchedule = new DiagnosticSchedule();
    private volatile long nextRetry;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>(Arrays.asList(SUCCESS, DIAGNOSTICS));
        NotificationBuffer pending = buffer;
        if (qualityRouting
                || (pending != null && pending.output.routeQuality && pending.size() > 0)) {
            relationships.add(BAD);
            relationships.add(UNCERTAIN);
        }
        return relationships;
    }

    @Override
    public void onPropertyModified(
            PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (QUALITY_ROUTING.equals(descriptor)) qualityRouting = Boolean.parseBoolean(newValue);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String name) {
        return new PropertyDescriptor.Builder()
                .name(name)
                .dynamic(true)
                .required(false)
                .description(
                        "NodeId or JSON tag options. NodeIds are case-sensitive; namespace indexes"
                            + " must match the server. Namespace URIs (nsu=) resolve on each new"
                            + " session.")
                .addValidator(
                        (subject, input, context) -> {
                            try {
                                TagDefinition.parse(subject, input, 1000, 100);
                                return new ValidationResult.Builder()
                                        .subject(subject)
                                        .valid(true)
                                        .build();
                            } catch (IllegalArgumentException e) {
                                return new ValidationResult.Builder()
                                        .subject(subject)
                                        .valid(false)
                                        .explanation(e.getMessage())
                                        .build();
                            }
                        })
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> errors = new ArrayList<>();
        try {
            TagList.parse(
                    context.getProperty(TAG_LIST).getValue(),
                    individualTags(context.getProperties()),
                    1000,
                    100);
        } catch (IllegalArgumentException e) {
            errors.add(
                    new ValidationResult.Builder()
                            .subject("Tags")
                            .valid(false)
                            .explanation(e.getMessage())
                            .build());
        }
        String endpoint = context.getProperty(ENDPOINT).getValue();
        try {
            java.net.URI uri = java.net.URI.create(endpoint);
            if (!"opc.tcp".equals(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null) throw new IllegalArgumentException();
        } catch (Exception e) {
            errors.add(
                    new ValidationResult.Builder()
                            .subject(ENDPOINT.getName())
                            .valid(false)
                            .explanation(
                                    "Use an opc.tcp URL with a host and no embedded credentials,"
                                            + " query or fragment")
                            .build());
        }
        OpcSecurity security = null;
        try {
            security =
                    new OpcSecurity(
                            context.getProperty(SECURITY_POLICY).getValue(),
                            context.getProperty(SECURITY_MODE).getValue(),
                            context.getProperty(AUTHENTICATION).getValue(),
                            UnencryptedPasswordPolicy.parse(
                                    context.getProperty(ALLOW_UNENCRYPTED_PASSWORD).getValue()));
        } catch (RuntimeException e) {
            errors.add(
                    new ValidationResult.Builder()
                            .subject("Security")
                            .valid(false)
                            .explanation(e.getMessage())
                            .build());
        }
        if (security != null) {
            if (security.requiresClientIdentity()) {
                if ("PEM/DER".equals(context.getProperty(CLIENT_IDENTITY_FORMAT).getValue())) {
                    for (PropertyDescriptor required : Arrays.asList(CERTIFICATE, PRIVATE_KEY))
                        requireProperty(context, required, errors);
                    for (PropertyDescriptor unused :
                            Arrays.asList(KEYSTORE, KEYSTORE_PASSWORD, KEY_ALIAS))
                        rejectProperty(context, unused, "Client Identity Format PEM/DER", errors);
                } else {
                    requireProperty(context, KEYSTORE, errors);
                    for (PropertyDescriptor unused : Arrays.asList(CERTIFICATE, PRIVATE_KEY))
                        rejectProperty(context, unused, "a JKS or PKCS12 client identity", errors);
                }
            }
            if (!"None".equals(context.getProperty(SERVER_CERTIFICATE_VALIDATION).getValue())
                    && (security.requiresClientIdentity() || security.usernameAuthentication())) {
                if ("PKI Directory".equals(context.getProperty(SERVER_TRUST_SOURCE).getValue())) {
                    if (security.requiresClientIdentity()) requireProperty(context, PKI, errors);
                    for (PropertyDescriptor unused : Arrays.asList(TRUSTSTORE, TRUSTSTORE_PASSWORD))
                        rejectProperty(
                                context, unused, "Server Trust Source PKI Directory", errors);
                } else if (security.requiresClientIdentity()) {
                    requireProperty(context, TRUSTSTORE, errors);
                }
            }
            if (security.usernameAuthentication()) {
                requireProperty(context, USERNAME, errors);
                requireProperty(context, PASSWORD, errors);
            }
        }
        try {
            long record = context.getProperty(RECORD_BYTES).asInteger();
            if (record > context.getProperty(BUFFER_BYTES).asInteger()
                    || record + 2 > context.getProperty(BATCH_BYTES).asInteger())
                errors.add(
                        new ValidationResult.Builder()
                                .subject("Byte limits")
                                .valid(false)
                                .explanation(
                                        "Maximum Record Bytes must fit Notification Buffer Bytes"
                                                + " and Maximum FlowFile Bytes minus two")
                                .build());
        } catch (RuntimeException ignored) {
            /* Individual property validators report invalid integers. */
        }
        String applicationUri = context.getProperty(APPLICATION_URI).getValue();
        if (applicationUri != null) {
            try {
                if (!java.net.URI.create(applicationUri).isAbsolute())
                    throw new IllegalArgumentException();
            } catch (IllegalArgumentException e) {
                errors.add(
                        new ValidationResult.Builder()
                                .subject(APPLICATION_URI.getName())
                                .valid(false)
                                .explanation("Use an absolute application URI")
                                .build());
            }
        }
        for (PropertyDescriptor path :
                Arrays.asList(CERTIFICATE, PRIVATE_KEY, KEYSTORE, TRUSTSTORE, PKI)) {
            String value = context.getProperty(path).getValue();
            if (value == null) continue;
            try {
                if (!java.nio.file.Paths.get(value).isAbsolute())
                    throw new IllegalArgumentException();
            } catch (Exception e) {
                errors.add(
                        new ValidationResult.Builder()
                                .subject(path.getName())
                                .valid(false)
                                .explanation("Use an absolute path on the NiFi host")
                                .build());
            }
        }
        return errors;
    }

    private static void requireProperty(
            ValidationContext context, PropertyDescriptor property, List<ValidationResult> errors) {
        if (context.getProperty(property).getValue() == null)
            errors.add(
                    new ValidationResult.Builder()
                            .subject(property.getName())
                            .valid(false)
                            .explanation(
                                    "Required for the selected security and authentication"
                                            + " settings")
                            .build());
    }

    private static void rejectProperty(
            ValidationContext context,
            PropertyDescriptor property,
            String source,
            List<ValidationResult> errors) {
        if (context.getProperty(property).getValue() != null)
            errors.add(
                    new ValidationResult.Builder()
                            .subject(property.getName())
                            .valid(false)
                            .explanation("Clear this property when using " + source)
                            .build());
    }

    private void initializeBuffer(ProcessContext context) {
        buffer =
                new NotificationBuffer(
                        context.getProperty(BUFFER).asInteger(),
                        context.getProperty(BUFFER_BYTES).asInteger(),
                        context.getProperty(RECORD_BYTES).asInteger(),
                        outputSettings(context));
        bufferBatchBytes = context.getProperty(BATCH_BYTES).asInteger();
        bufferEndpoint = context.getProperty(ENDPOINT).getValue();
        bufferSecurityPolicy = context.getProperty(SECURITY_POLICY).getValue();
        bufferSecurityMode = context.getProperty(SECURITY_MODE).getValue();
        replaceBufferWhenDrained = false;
    }

    @OnScheduled
    public void start(ProcessContext context) throws Exception {
        synchronized (lifecycle) {
            cancelStopDrain();
            sessionFactory = null;
            if (buffer == null || buffer.size() == 0) {
                initializeBuffer(context);
            } else {
                // Preserve old data and its source identity, even if the next configuration
                // differs.
                replaceBufferWhenDrained = true;
            }
            recovery.set(false);
            nextRetry = 0;
            diagnosticSchedule = new DiagnosticSchedule();
            scheduledContext = context;
            running = true;
        }
    }

    private boolean ownsSubscription() {
        ProcessContext context = scheduledContext;
        return !getNodeTypeProvider().isConfiguredForClustering()
                || (context != null
                        && context.isConnectedToCluster()
                        && getNodeTypeProvider().isPrimary());
    }

    @OnPrimaryNodeStateChange
    public void primaryChanged(PrimaryNodeState ignored) {
        // NiFi dispatches these notifications asynchronously. A delayed event must not
        // override the current ownership state or disable a re-elected primary.
        if (!ownsSubscription()) {
            cancelConnection();
        } else {
            nextRetry = 0;
        }
    }

    protected AutoCloseable openConnection(ProcessContext context, ConnectionAttempt generation)
            throws Exception {
        OpcSettings settings = settings(context);
        NotificationBuffer destination = buffer;
        return new OpcConnection(
                settings,
                (tag, value) -> accept(destination, tag, value, generation),
                () -> generation.ifActive(() -> recovery.set(true)),
                () ->
                        generation.ifActive(
                                () -> {
                                    dataLost.incrementAndGet();
                                    totalDataLost.incrementAndGet();
                                }),
                generation,
                diagnostics);
    }

    private static OpcSettings settings(ProcessContext context) {
        List<TagDefinition> tags =
                TagList.parse(
                        context.getProperty(TAG_LIST).getValue(),
                        individualTags(context.getProperties()),
                        context.getProperty(SAMPLING).asInteger(),
                        context.getProperty(SERVER_QUEUE).asInteger());
        return new OpcSettings(
                        context.getProperty(ENDPOINT).getValue(),
                        context.getProperty(APPLICATION_URI).getValue(),
                        path(context, CERTIFICATE),
                        path(context, PRIVATE_KEY),
                        Optional.ofNullable(context.getProperty(KEY_PASSWORD).getValue())
                                .orElse(""),
                        path(context, PKI),
                        context.getProperty(USERNAME).getValue(),
                        context.getProperty(PASSWORD).getValue(),
                        context.getProperty(PUBLISHING).asInteger(),
                        context.getProperty(TIMEOUT).asInteger(),
                        tags,
                        new OpcSecurity(
                                context.getProperty(SECURITY_POLICY).getValue(),
                                context.getProperty(SECURITY_MODE).getValue(),
                                context.getProperty(AUTHENTICATION).getValue(),
                                UnencryptedPasswordPolicy.parse(
                                        context.getProperty(ALLOW_UNENCRYPTED_PASSWORD)
                                                .getValue())),
                        ServerCertificateValidation.parse(
                                context.getProperty(SERVER_CERTIFICATE_VALIDATION).getValue()))
                .withKeyStores(
                        keyStore(
                                context,
                                CLIENT_IDENTITY_FORMAT,
                                "PEM/DER",
                                KEYSTORE,
                                KEYSTORE_PASSWORD),
                        context.getProperty(KEY_ALIAS).getValue(),
                        keyStore(
                                context,
                                SERVER_TRUST_SOURCE,
                                "PKI Directory",
                                TRUSTSTORE,
                                TRUSTSTORE_PASSWORD))
                .withOutput(outputSettings(context));
    }

    private static KeyStoreSettings keyStore(
            ProcessContext context,
            PropertyDescriptor source,
            String fileSource,
            PropertyDescriptor file,
            PropertyDescriptor password) {
        String type = context.getProperty(source).getValue();
        return fileSource.equals(type)
                ? null
                : new KeyStoreSettings(
                        type, path(context, file), context.getProperty(password).getValue());
    }

    private static OutputSettings outputSettings(ProcessContext context) {
        return new OutputSettings(
                context.getProperty(OUTPUT_FIELDS).getValue(),
                context.getProperty(OUTPUT_FORMAT).getValue(),
                context.getProperty(TIMESTAMP_FORMAT).getValue(),
                context.getProperty(QUALITY_ROUTING).asBoolean(),
                context.getProperty(BATCH_DELAY).asInteger());
    }

    protected AutoCloseable verifyConnection(
            OpcSettings settings, ConnectionAttempt generation, ConnectionDiagnostics report)
            throws Exception {
        return new OpcConnection(
                settings, (tag, value) -> {}, () -> {}, () -> {}, generation, report);
    }

    @Override
    public List<ConfigVerificationResult> verify(
            ProcessContext context,
            org.apache.nifi.logging.ComponentLog logger,
            Map<String, String> attributes) {
        ConnectionDiagnostics report = new ConnectionDiagnostics(value -> redact(context, value));
        ConnectionAttempt generation = new ConnectionAttempt();
        ConfigVerificationResult.Builder result =
                new ConfigVerificationResult.Builder()
                        .verificationStepName("OPC UA subscription verification");
        try (AutoCloseable verified = verifyConnection(settings(context), generation, report)) {
            result.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .explanation(
                            "Opened session, resolved tags and created monitored items. Temporary"
                                    + " subscription is closed after verification. "
                                    + redact(context, report.snapshot().toString()));
        } catch (Exception e) {
            result.outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation(
                            failureDescription(context, e)
                                    + "; "
                                    + redact(context, report.snapshot().toString()));
        } finally {
            generation.cancel();
        }
        return Collections.singletonList(result.build());
    }

    private static Map<String, String> individualTags(Map<PropertyDescriptor, String> properties) {
        Map<String, String> tags = new LinkedHashMap<>();
        properties.forEach(
                (descriptor, value) -> {
                    if (descriptor.isDynamic()) tags.put(descriptor.getName(), value);
                });
        return tags;
    }

    private static java.nio.file.Path path(ProcessContext context, PropertyDescriptor property) {
        String value = context.getProperty(property).getValue();
        return value == null ? null : java.nio.file.Paths.get(value);
    }

    void accept(NotificationBuffer destination, TagDefinition tag, DataValue value) {
        accept(destination, tag, value, null);
    }

    void accept(
            NotificationBuffer destination,
            TagDefinition tag,
            DataValue value,
            ConnectionAttempt generation) {
        if (generation != null && (generation.isCancelled() || !running || !ownsSubscription()))
            return;
        int limit = destination.encodingLimit();
        if (limit == 0) {
            acceptIfActive(generation, () -> recordDrop(false, false));
            return;
        }
        try {
            // Encoding runs outside the cancellation monitor. Only final admission is fenced.
            byte[] record = encodeNotification(tag, value, limit, destination.output);
            acceptIfActive(
                    generation,
                    () -> {
                        if (!destination.offer(
                                record, destination.output.relationship(value.getStatusCode())))
                            recordDrop(false, false);
                    });
        } catch (Exception e) {
            boolean sizeLimit = isOversized(e);
            acceptIfActive(
                    generation,
                    () ->
                            recordDrop(
                                    sizeLimit && limit == destination.maximumRecordBytes(),
                                    !sizeLimit));
        }
    }

    byte[] encodeNotification(TagDefinition tag, DataValue value, int limit, OutputSettings output)
            throws java.io.IOException {
        return NotificationJson.encode(tag, value, limit, output);
    }

    private void acceptIfActive(ConnectionAttempt generation, Runnable admission) {
        if (generation == null) admission.run();
        else
            generation.ifActive(
                    () -> {
                        if (running && ownsSubscription()) admission.run();
                    });
    }

    private void recordDrop(boolean tooLarge, boolean encodingFailure) {
        dropped.incrementAndGet();
        totalDropped.incrementAndGet();
        if (tooLarge) oversized.incrementAndGet();
        if (encodingFailure) encodingErrors.incrementAndGet();
    }

    private static boolean isOversized(Throwable error) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = error; cause != null && seen.add(cause); cause = cause.getCause())
            if (cause instanceof NotificationJson.RecordTooLargeException) return true;
        return false;
    }

    void acceptForTest(TagDefinition tag, DataValue value) {
        accept(buffer, tag, value);
    }

    private static String failureDescription(ProcessContext context, Throwable failure) {
        StringJoiner details = new StringJoiner("; caused by ");
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = failure;
                cause != null && seen.add(cause);
                cause = cause.getCause()) {
            if ((cause instanceof ExecutionException || cause instanceof CompletionException)
                    && cause.getCause() != null) continue;
            String detail = cause.getClass().getSimpleName();
            if (cause instanceof UaExceptionStatus)
                detail += " [" + ((UaExceptionStatus) cause).getStatusCode() + "]";
            String message = cause.getMessage();
            if (message != null && !message.isEmpty()) detail += ": " + message;
            details.add(detail);
        }
        String description = redact(context, details.toString());
        return description.length() > 4096 ? description.substring(0, 4096) + "…" : description;
    }

    private static String redact(ProcessContext context, String description) {
        List<String> passwords = new ArrayList<>();
        for (PropertyDescriptor property :
                Arrays.asList(PASSWORD, KEY_PASSWORD, KEYSTORE_PASSWORD, TRUSTSTORE_PASSWORD)) {
            String password = context.getProperty(property).getValue();
            if (password != null && !password.isEmpty()) passwords.add(password);
        }
        // Redact whole secrets before their prefixes or any newline normalization.
        passwords.sort(Comparator.comparingInt(String::length).reversed());
        for (String password : passwords) description = description.replace(password, "[redacted]");
        return description.replaceAll("[\\p{Cntrl}\\u2028\\u2029]", " ");
    }

    private static Object redactMetadata(ProcessContext context, Object value) {
        if (value instanceof String) return redact(context, (String) value);
        if (value instanceof Map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            ((Map<?, ?>) value)
                    .forEach(
                            (key, item) ->
                                    copy.put(String.valueOf(key), redactMetadata(context, item)));
            return copy;
        }
        if (value instanceof List) {
            List<Object> copy = new ArrayList<>();
            for (Object item : (List<?>) value) copy.add(redactMetadata(context, item));
            return copy;
        }
        return value;
    }

    Map<String, Object> diagnosticSnapshot(ProcessContext context) {
        Map<String, Object> report = new LinkedHashMap<>();
        Map<String, Object> current = diagnostics.snapshot();
        report.put("observedAt", java.time.Instant.now().toString());
        report.put(
                "state",
                !running
                        ? "STOPPED"
                        : !ownsSubscription()
                                ? "STANDBY"
                                : recovery.get()
                                        ? "RETRYING"
                                        : connection == null
                                                ? "WAITING"
                                                : Boolean.TRUE.equals(current.get("sessionActive"))
                                                        ? "SUBSCRIBED"
                                                        : "RECONNECTING");
        report.put("connectionAttempts", connectionAttempts.get());
        report.put("successfulConnections", successfulConnections.get());
        report.put("subscriptionRecreations", Math.max(0, successfulConnections.get() - 1));
        report.put("notificationsDropped", totalDropped.get());
        report.put("oversizedNotifications", oversized.get());
        report.put("encodingFailures", encodingErrors.get());
        report.put("serverDataLossEvents", totalDataLost.get());
        report.put("lastFailure", lastFailure);
        report.put("connection", current);
        if (buffer != null) {
            Map<String, Object> buffered = buffer.snapshot();
            buffered.put("endpoint", bufferEndpoint);
            buffered.put("securityPolicy", bufferSecurityPolicy);
            buffered.put("securityMode", bufferSecurityMode);
            buffered.put("maximumFlowFileBytes", bufferBatchBytes);
            buffered.put("maximumRecordBytes", buffer.maximumRecordBytes());
            report.put("buffer", buffered);
        }
        return report;
    }

    @Override
    public final void onTrigger(ProcessContext context, ProcessSessionFactory factory)
            throws ProcessException {
        // Retain the factory (never a session shared across threads) for stop-time delivery.
        if (running) sessionFactory = factory;
        ProcessSession session = factory.createSession();
        try {
            onTrigger(context, session);
            session.commitAsync();
        } catch (Throwable t) {
            try {
                session.rollback(true);
            } catch (Throwable cleanup) {
                if (cleanup != t) t.addSuppressed(cleanup);
            }
            throw t;
        }
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        synchronized (lifecycle) {
            if (!running || !ownsSubscription() || buffer == null) {
                closeConnection();
                context.yield();
                return;
            }
            if (replaceBufferWhenDrained && buffer.size() == 0) {
                initializeBuffer(context);
            }
            // Stop the failed generation before draining, so it cannot keep filling the buffer.
            if (recovery.get() && connection != null) closeConnection();
            // Drain retained data even if the OPC server is unavailable.
            if (buffer.size() == 0
                    && (connection == null || recovery.get())
                    && (nextRetry == 0 || System.nanoTime() - nextRetry >= 0)) {
                closeConnection();
                recovery.set(false);
                ConnectionAttempt generation = new ConnectionAttempt();
                synchronized (connectionLock) {
                    attempt = generation;
                    if (!running || !ownsSubscription()) generation.cancel();
                }
                diagnostics = new ConnectionDiagnostics(value -> redact(context, value));
                connectionAttempts.incrementAndGet();
                if (!running || !ownsSubscription()) generation.cancel();
                try {
                    generation.check();
                    publishConnection(openConnection(context, generation), generation);
                    successfulConnections.incrementAndGet();
                } catch (Exception e) {
                    generation.cancel();
                    closeConnection();
                    if (running && ownsSubscription()) {
                        recovery.set(true);
                        lastFailure = failureDescription(context, e);
                        getLogger()
                                .error(
                                        "Could not establish OPC UA subscription ({}); retrying"
                                                + " after configured delay",
                                        new Object[] {
                                            lastFailure
                                                    + "; stage="
                                                    + diagnostics.snapshot().get("stage")
                                                    + "; serverCertificateValidation="
                                                    + diagnostics
                                                            .snapshot()
                                                            .get("serverCertificateValidation")
                                                    + "; advertised endpoints="
                                                    + redact(
                                                            context,
                                                            String.valueOf(
                                                                    diagnostics
                                                                            .snapshot()
                                                                            .get(
                                                                                    "advertisedEndpoints")))
                                        });
                        context.yield();
                    }
                }
                nextRetry = System.nanoTime() + context.getProperty(RETRY).asInteger() * 1000000L;
            }
            if (!running || !ownsSubscription()) return;
            long lost = dropped.getAndSet(0);
            if (lost > 0) {
                session.adjustCounter("OPC UA notifications dropped", lost, true);
                getLogger()
                        .error(
                                "Dropped {} OPC UA notifications due to local buffer overflow or"
                                        + " encoding failure",
                                new Object[] {lost});
            }
            long gaps = dataLost.getAndSet(0);
            if (gaps > 0) {
                session.adjustCounter("OPC UA server data loss events", gaps, true);
                getLogger()
                        .error(
                                "OPC UA server could not republish notifications ({} events); there"
                                        + " is a data gap",
                                new Object[] {gaps});
            }
            final NotificationBuffer pending = buffer;
            final int batchSize = context.getProperty(BATCH).asInteger();
            boolean ready;
            try {
                // A commit in progress is not an empty queue. Wait briefly for its callback
                // instead of imposing NiFi's (usually much longer) Yield Duration.
                long deadline = System.nanoTime() + ACTIVE_WAIT_NANOS;
                pending.awaitAvailable(deadline);
                ready =
                        replaceBufferWhenDrained
                                || pending.awaitBatch(batchSize, bufferBatchBytes, deadline);
            } catch (java.util.concurrent.TimeoutException e) {
                return; // The bounded wait prevents a zero-period scheduling spin.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!running || !ownsSubscription()) return;
            Relationship destination = pending.nextRelationship();
            if (pending.size() > 0 && !context.getAvailableRelationships().contains(destination)) {
                context.yield();
                return;
            }
            final DiagnosticSchedule reportSchedule = diagnosticSchedule;
            final int diagnosticInterval = context.getProperty(DIAGNOSTICS_INTERVAL).asInteger();
            boolean submitted = false;
            AtomicBoolean settled = new AtomicBoolean();
            List<byte[]> records = Collections.emptyList();
            boolean emitDiagnostic = false;
            try {
                records =
                        ready
                                ? pending.reserve(batchSize, bufferBatchBytes)
                                : Collections.emptyList();
                emitDiagnostic =
                        diagnosticInterval > 0
                                && (reportSchedule.next == 0
                                        || System.nanoTime() - reportSchedule.next >= 0)
                                && diagnosticsPending.compareAndSet(false, true);
                if (records.isEmpty() && !emitDiagnostic) {
                    if (pending.size() == 0) context.yield();
                    return;
                }
                final int recordCount = records.size();
                final boolean diagnosticClaimed = emitDiagnostic;
                if (emitDiagnostic) {
                    byte[] report =
                            JSON.writeValueAsBytes(
                                    redactMetadata(context, diagnosticSnapshot(context)));
                    FlowFile diagnostic = session.create();
                    diagnostic = session.write(diagnostic, out -> out.write(report));
                    Map<String, String> diagnosticAttributes = new HashMap<>();
                    diagnosticAttributes.put("mime.type", "application/json");
                    diagnosticAttributes.put("opcua.type", "diagnostics");
                    diagnostic = session.putAllAttributes(diagnostic, diagnosticAttributes);
                    session.transfer(diagnostic, DIAGNOSTICS);
                }
                if (!records.isEmpty()) {
                    NotificationBatch.write(
                            session,
                            records,
                            bufferEndpoint,
                            bufferSecurityPolicy,
                            bufferSecurityMode,
                            pending.output,
                            destination);
                }
                session.commitAsync(
                        () -> {
                            if (settled.compareAndSet(false, true)) {
                                if (recordCount > 0) pending.acknowledge(recordCount);
                                if (diagnosticClaimed) {
                                    reportSchedule.next =
                                            System.nanoTime() + diagnosticInterval * 1000000000L;
                                    diagnosticsPending.set(false);
                                }
                            }
                        },
                        error -> {
                            if (settled.compareAndSet(false, true)) {
                                if (recordCount > 0) pending.release();
                                if (diagnosticClaimed) diagnosticsPending.set(false);
                                getLogger()
                                        .error(
                                                "OPC UA FlowFile commit failed; notifications"
                                                        + " retained for retry");
                            }
                        });
                submitted = true;
            } catch (Exception | Error e) {
                if (!submitted && !settled.get()) {
                    try {
                        session.rollback();
                    } catch (Exception | Error cleanup) {
                        if (cleanup != e) e.addSuppressed(cleanup);
                    } finally {
                        if (settled.compareAndSet(false, true)) {
                            if (!records.isEmpty()) pending.release();
                            if (emitDiagnostic) diagnosticsPending.set(false);
                        }
                    }
                }
                if (e instanceof Error) throw (Error) e;
                throw new ProcessException(
                        "Could not commit OPC UA batch; notifications retained for retry", e);
            }
        }
    }

    @OnUnscheduled
    public void unscheduled() {
        running = false;
        cancelConnection();
    }

    @OnStopped
    public void stop() {
        unscheduled();
        String outcome = drainBuffered();
        int remaining = buffer == null ? 0 : buffer.size();
        if (remaining > 0)
            getLogger()
                    .warn(
                            "Stopped with {} uncommitted notifications in memory ({}); retained for"
                                + " Start and drained before the next subscription; lost if the JVM"
                                + " exits or processor is removed before commit",
                            new Object[] {remaining, outcome});
        long lost = dropped.getAndSet(0);
        if (lost > 0)
            getLogger()
                    .error(
                            "{} additional notifications were dropped before stop",
                            new Object[] {lost});
    }

    private String drainBuffered() {
        NotificationBuffer pending = buffer;
        if (pending == null || pending.size() == 0) return "drained";
        if (Thread.currentThread().isInterrupted()) return "stop interrupted";
        ProcessContext context = scheduledContext;
        if (context == null) return "no scheduled context";
        int timeout = context.getProperty(STOP_DRAIN_TIMEOUT).asInteger();
        if (timeout == 0) return "stop draining disabled";
        ProcessSessionFactory factory = sessionFactory;
        if (factory == null) return "no session factory available";
        StopDrain drain;
        try {
            synchronized (drainLock) {
                if (stopDrain != null && !stopDrain.isFinished()) {
                    stopDrain.cancel();
                    return "previous stop drain is still completing";
                }
                drain =
                        new StopDrain(
                                pending,
                                factory,
                                context,
                                () -> !running && ownsSubscription(),
                                context.getProperty(BATCH).asInteger(),
                                bufferBatchBytes,
                                bufferEndpoint,
                                bufferSecurityPolicy,
                                bufferSecurityMode,
                                timeout,
                                "opcua-stop-drain-" + getIdentifier());
                stopDrain = drain;
                drain.start();
            }
            return drain.await();
        } catch (RuntimeException e) {
            return "could not drain (" + e.getClass().getSimpleName() + ")";
        }
    }

    private void cancelStopDrain() {
        synchronized (drainLock) {
            if (stopDrain != null) {
                stopDrain.cancel();
                if (stopDrain.isFinished()) stopDrain = null;
            }
        }
    }

    private void cancelConnection() {
        ConnectionAttempt current = attempt;
        if (current != null) current.cancel();
        closeConnection();
    }

    @OnRemoved
    public void removed() {
        unscheduled();
        cancelStopDrain();
        synchronized (lifecycle) {
            buffer = null;
            bufferEndpoint = null;
            bufferSecurityPolicy = null;
            bufferSecurityMode = null;
            scheduledContext = null;
            sessionFactory = null;
        }
    }

    private void closeConnection() {
        synchronized (connectionLock) {
            if (attempt != null) attempt.cancel();
            AutoCloseable owned = connection;
            connection = null;
            closeResource(owned);
        }
    }

    private void publishConnection(AutoCloseable opened, ConnectionAttempt generation) {
        synchronized (connectionLock) {
            if (attempt != generation
                    || generation.isCancelled()
                    || !running
                    || !ownsSubscription()) {
                generation.cancel();
                closeResource(opened);
                throw new CancellationException("OPC UA connection cancelled before publication");
            }
            connection = opened;
        }
    }

    private void closeResource(AutoCloseable owned) {
        if (owned == null) return;
        try {
            owned.close();
        } catch (Exception e) {
            getLogger()
                    .warn(
                            "OPC UA connection cleanup failed ({})",
                            new Object[] {e.getClass().getSimpleName()});
        }
    }
}
