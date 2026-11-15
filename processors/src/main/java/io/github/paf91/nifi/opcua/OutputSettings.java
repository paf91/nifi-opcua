/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.apache.nifi.processor.Relationship;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;

import java.util.*;

/** Immutable output contract retained with a buffer, including across configuration changes. */
final class OutputSettings {
    static final String DEFAULT_FIELDS =
            "tag,nodeId,configuredNodeId,namespaceUri,value,valueType,statusCode,quality,sourceTimestamp,receivedTimestamp";
    static final String COMPACT_FIELDS = "tag,value,statusCode,sourceTimestamp";

    enum Field {
        tag,
        nodeId,
        configuredNodeId,
        namespaceUri,
        value,
        valueType,
        statusCode,
        quality,
        sourceTimestamp,
        receivedTimestamp,
        serverTimestamp,
        dataType,
        engineeringUnits
    }

    static final OutputSettings DEFAULT =
            new OutputSettings(DEFAULT_FIELDS, "JSON Array", "ISO-8601", false, 0);
    private final EnumSet<Field> fields;
    final String fieldNames;
    final boolean ndjson;
    final boolean epochMillis;
    final boolean routeQuality;
    final long batchDelayNanos;

    OutputSettings(
            String names, String format, String timestamps, boolean routeQuality, int delayMillis) {
        fields = parseFields(names);
        fieldNames = String.join(",", fields.stream().map(Enum::name).toArray(String[]::new));
        if (!"JSON Array".equals(format) && !"NDJSON".equals(format))
            throw new IllegalArgumentException("Output Format must be JSON Array or NDJSON");
        if (!"ISO-8601".equals(timestamps) && !"Epoch Milliseconds".equals(timestamps))
            throw new IllegalArgumentException("Unknown timestamp format");
        if (delayMillis < 0 || delayMillis > 60000)
            throw new IllegalArgumentException("Batch delay must be between 0 and 60000 ms");
        ndjson = "NDJSON".equals(format);
        epochMillis = "Epoch Milliseconds".equals(timestamps);
        this.routeQuality = routeQuality;
        batchDelayNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(delayMillis);
    }

    static EnumSet<Field> parseFields(String names) {
        EnumSet<Field> fields = EnumSet.noneOf(Field.class);
        if (names == null || names.trim().isEmpty())
            throw new IllegalArgumentException("Output Fields must not be empty");
        for (String part : names.split(",", -1)) {
            Field field;
            try {
                field = Field.valueOf(part.trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown output field: " + part.trim());
            }
            if (!fields.add(field))
                throw new IllegalArgumentException("Duplicate output field: " + field);
        }
        if (!fields.contains(Field.value)
                || !(fields.contains(Field.tag) || fields.contains(Field.nodeId)))
            throw new IllegalArgumentException(
                    "Include value and at least one identity field: tag or nodeId");
        return fields;
    }

    boolean includes(Field field) {
        return fields.contains(field);
    }

    boolean metadata() {
        return includes(Field.dataType) || includes(Field.engineeringUnits);
    }

    Relationship relationship(StatusCode status) {
        if (!routeQuality || (status != null && status.isGood())) return ConsumeOPCUA.SUCCESS;
        return status != null && status.isBad() ? ConsumeOPCUA.BAD : ConsumeOPCUA.UNCERTAIN;
    }
}
