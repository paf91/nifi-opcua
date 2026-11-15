/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.*;

import java.io.*;
import java.lang.reflect.Array;
import java.time.Instant;
import java.util.*;

final class NotificationJson {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();
    static final int MAX_VALUE_DEPTH = 128;

    static byte[] encode(TagDefinition tag, DataValue value) throws java.io.IOException {
        return encode(tag, value, Integer.MAX_VALUE);
    }

    static byte[] encode(TagDefinition tag, DataValue value, int maximumBytes) throws IOException {
        return encode(tag, value, maximumBytes, OutputSettings.DEFAULT);
    }

    static byte[] encode(
            TagDefinition tag, DataValue value, int maximumBytes, OutputSettings settings)
            throws IOException {
        Object raw = value.getValue() == null ? null : value.getValue().getValue();
        StatusCode status = value.getStatusCode();
        LimitedOutput output = new LimitedOutput(maximumBytes);
        try (JsonGenerator generator = JSON.getFactory().createGenerator(output)) {
            generator.writeStartObject();
            if (settings.includes(OutputSettings.Field.tag))
                generator.writeStringField("tag", tag.alias());
            if (settings.includes(OutputSettings.Field.nodeId))
                generator.writeStringField("nodeId", tag.nodeIdText(maximumBytes));
            if (tag.address().namespaceUri() != null) {
                if (settings.includes(OutputSettings.Field.configuredNodeId))
                    generator.writeStringField("configuredNodeId", tag.address().text());
                if (settings.includes(OutputSettings.Field.namespaceUri))
                    generator.writeStringField("namespaceUri", tag.address().namespaceUri());
            }
            generator.writeFieldName("value");
            writeValue(generator, raw);
            if (settings.includes(OutputSettings.Field.valueType))
                generator.writeStringField(
                        "valueType", raw == null ? null : raw.getClass().getSimpleName());
            if (settings.includes(OutputSettings.Field.statusCode))
                generator.writeStringField(
                        "statusCode", status == null ? null : statusText(status.getValue()));
            if (settings.includes(OutputSettings.Field.quality))
                generator.writeStringField(
                        "quality",
                        status == null
                                ? "Unknown"
                                : status.isGood() ? "Good" : status.isBad() ? "Bad" : "Uncertain");
            if (settings.includes(OutputSettings.Field.sourceTimestamp))
                writeTimestamp(generator, "sourceTimestamp", value.getSourceTime(), settings);
            if (settings.includes(OutputSettings.Field.serverTimestamp))
                writeTimestamp(generator, "serverTimestamp", value.getServerTime(), settings);
            if (settings.includes(OutputSettings.Field.receivedTimestamp)) {
                if (settings.epochMillis)
                    generator.writeNumberField("receivedTimestamp", System.currentTimeMillis());
                else generator.writeStringField("receivedTimestamp", Instant.now().toString());
            }
            if (settings.includes(OutputSettings.Field.dataType))
                generator.writeStringField("dataType", tag.metadata().dataType);
            if (settings.includes(OutputSettings.Field.engineeringUnits))
                tag.metadata().writeUnits(generator);
            generator.writeEndObject();
        }
        return output.bytes.toByteArray();
    }

    private static String statusText(long value) {
        // OPC UA status codes are UInt32; retain the old representation for unusual SDK values.
        if ((value & 0xffffffff00000000L) != 0)
            return "0x" + Long.toHexString(value).toUpperCase(Locale.ROOT);
        char[] text = new char[10];
        text[0] = '0';
        text[1] = 'x';
        for (int i = 9; i >= 2; i--, value >>>= 4) text[i] = HEX[(int) (value & 15)];
        return new String(text);
    }

    private static void writeValue(JsonGenerator generator, Object value) throws IOException {
        writeValue(generator, value, 0);
    }

    static void writeValue(JsonGenerator generator, Object value, int depth) throws IOException {
        if (depth > MAX_VALUE_DEPTH) throw new IOException("OPC UA value nesting is too deep");
        if (OpcValue.writeMatrix(generator, value, depth)) return;
        if (value instanceof Variant)
            writeValue(generator, ((Variant) value).getValue(), depth + 1);
        else if (value instanceof ByteString) {
            ByteString binary = (ByteString) value;
            if (binary.isNull()) generator.writeNull();
            else generator.writeBinary(binary.bytes());
        } else if (value != null && value.getClass().isArray()) {
            generator.writeStartArray();
            if (depth == MAX_VALUE_DEPTH && Array.getLength(value) > 0)
                throw new IOException("OPC UA value nesting is too deep");
            if (value instanceof double[]) {
                for (double n : (double[]) value) {
                    if (Double.isFinite(n)) generator.writeNumber(n);
                    else generator.writeString(Double.toString(n));
                }
            } else if (value instanceof float[]) {
                for (float n : (float[]) value) {
                    if (Float.isFinite(n)) generator.writeNumber(n);
                    else generator.writeString(Float.toString(n));
                }
            } else if (value instanceof int[]) {
                for (int n : (int[]) value) generator.writeNumber(n);
            } else if (value instanceof long[]) {
                for (long n : (long[]) value) generator.writeString(Long.toString(n));
            } else if (value instanceof short[]) {
                for (short n : (short[]) value) generator.writeNumber(n);
            } else if (value instanceof byte[]) {
                for (byte n : (byte[]) value) generator.writeNumber(n);
            } else if (value instanceof boolean[]) {
                for (boolean n : (boolean[]) value) generator.writeBoolean(n);
            } else {
                for (int i = 0; i < Array.getLength(value); i++)
                    writeValue(generator, Array.get(value, i), depth + 1);
            }
            generator.writeEndArray();
        } else if (value instanceof ExtensionObject) {
            ExtensionObject extension = (ExtensionObject) value;
            Object body = extension.getBody();
            generator.writeStartObject();
            generator.writeStringField(
                    "encoding",
                    body instanceof ByteString
                            ? "opcua-binary"
                            : body instanceof XmlElement ? "opcua-xml" : "opcua-json");
            generator.writeStringField(
                    OpcValue.jsonEncoded(extension) ? "typeId" : "encodingId",
                    nodeIdText(
                            OpcValue.encodingId(extension),
                            ((LimitedOutput) generator.getOutputTarget()).limit));
            generator.writeFieldName("body");
            // Preserve opaque bodies without decoding them or materializing a display string.
            writeValue(generator, body, depth + 1);
            generator.writeEndObject();
        } else if (value instanceof XmlElement) {
            generator.writeString(((XmlElement) value).getFragment());
        } else if (value instanceof NodeId) {
            generator.writeString(
                    nodeIdText(
                            (NodeId) value, ((LimitedOutput) generator.getOutputTarget()).limit));
        } else writeScalar(generator, normalize(value));
    }

    private static void writeScalar(JsonGenerator generator, Object value) throws IOException {
        // Avoid ObjectMapper.writeValue(): it constructs a provider and flushes per element.
        if (value == null) generator.writeNull();
        else if (value instanceof String) generator.writeString((String) value);
        else if (value instanceof Boolean) generator.writeBoolean((Boolean) value);
        else if (value instanceof Integer || value instanceof Short || value instanceof Byte)
            generator.writeNumber(((Number) value).intValue());
        else if (value instanceof Long) generator.writeNumber((Long) value); // normalized UInt32
        else if (value instanceof Double) generator.writeNumber((Double) value);
        else if (value instanceof Float) generator.writeNumber((Float) value);
        else if (value instanceof Map) {
            generator.writeStartObject();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                generator.writeFieldName((String) entry.getKey());
                writeScalar(generator, entry.getValue());
            }
            generator.writeEndObject();
        } else throw new IOException("Unsupported normalized OPC UA value");
    }

    private static void writeTimestamp(
            JsonGenerator generator, String field, DateTime value, OutputSettings settings)
            throws IOException {
        if (value == null || value.getUtcTime() == 0) generator.writeNullField(field);
        else if (settings.epochMillis)
            generator.writeNumberField(
                    field, Math.floorDiv(value.getUtcTime(), 10000L) - 11644473600000L);
        else generator.writeStringField(field, timestamp(value));
    }

    private static String nodeIdText(NodeId value, int limit) throws RecordTooLargeException {
        checkNodeIdSize(value, limit);
        return value.toParseableString();
    }

    static void checkNodeIdSize(NodeId value, int limit) throws RecordTooLargeException {
        Object identifier = value.getIdentifier();
        if ((identifier instanceof String && ((String) identifier).length() > limit)
                || (identifier instanceof ByteString
                        && ((ByteString) identifier).length() > limit * 3L / 4))
            throw new RecordTooLargeException();
    }

    static final class RecordTooLargeException extends IOException {
        RecordTooLargeException() {
            super("Encoded OPC UA notification exceeds Maximum Record Bytes");
        }
    }

    private static final class LimitedOutput extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final int limit;

        LimitedOutput(int limit) {
            this.limit = limit;
        }

        @Override
        public void write(int value) throws IOException {
            if (bytes.size() >= limit) throw new RecordTooLargeException();
            bytes.write(value);
        }

        @Override
        public void write(byte[] value, int offset, int length) throws IOException {
            if (length > limit - bytes.size()) throw new RecordTooLargeException();
            bytes.write(value, offset, length);
        }
    }

    private static String timestamp(DateTime date) {
        if (date == null || date.getUtcTime() == 0) return null;
        // OPC UA uses 100 ns ticks since 1601. Dividing Java milliseconds by 1000
        // truncates negative seconds in both supported SDKs, shifting pre-1970 times.
        long ticks = date.getUtcTime();
        return Instant.ofEpochSecond(
                        Math.floorDiv(ticks, 10000000L) - 11644473600L,
                        Math.floorMod(ticks, 10000000L) * 100L)
                .toString();
    }

    static Object normalize(Object value) {
        if (value == null) return null;
        if (value instanceof ULong)
            return value.toString(); // exact beyond JSON/JavaScript integer precision
        if (value instanceof UInteger || value instanceof UShort || value instanceof UByte)
            return ((Number) value).longValue();
        if (value instanceof Long) return value.toString();
        if (value instanceof Double && !Double.isFinite((Double) value)) return value.toString();
        if (value instanceof Float && !Float.isFinite((Float) value)) return value.toString();
        if (value instanceof String
                || value instanceof Boolean
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Float
                || value instanceof Double) return value;
        if (value instanceof ByteString) {
            ByteString bytes = (ByteString) value;
            return bytes.isNull() ? null : Base64.getEncoder().encodeToString(bytes.bytes());
        }
        if (value instanceof DateTime) return timestamp((DateTime) value);
        if (value instanceof NodeId) return ((NodeId) value).toParseableString();
        if (value instanceof LocalizedText) {
            LocalizedText text = (LocalizedText) value;
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("text", text.getText());
            result.put("locale", text.getLocale());
            return result;
        }
        if (value instanceof Variant) return normalize(((Variant) value).getValue());
        if (value.getClass().isArray()) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < Array.getLength(value); i++)
                values.add(normalize(Array.get(value, i)));
            return values;
        }
        // Unknown implementations can allocate arbitrarily in toString(). Do not invoke them.
        Map<String, Object> fallback = new LinkedHashMap<>();
        boolean boundedDisplay = value instanceof UUID || value instanceof StatusCode;
        fallback.put("encoding", boundedDisplay ? "display" : "unsupported");
        fallback.put("type", value.getClass().getName());
        if (boundedDisplay) fallback.put("text", value.toString());
        return fallback;
    }
}
