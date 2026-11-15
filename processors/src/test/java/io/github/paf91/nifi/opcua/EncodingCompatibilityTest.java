/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.*;

import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.*;

class EncodingCompatibilityTest {
    @Test
    void directBoxedFloatingPointArraysPreserveNullsSignedZeroAndSpecialValues() throws Exception {
        assertArrayJson(
                new Double[] {
                    null,
                    -0.0,
                    0.0,
                    -Double.MIN_VALUE,
                    Double.MIN_VALUE,
                    -Double.MAX_VALUE,
                    Double.MAX_VALUE,
                    Double.NaN,
                    Double.NEGATIVE_INFINITY,
                    Double.POSITIVE_INFINITY
                },
                "[null,-0.0,0.0,-4.9E-324,4.9E-324,-1.7976931348623157E308,"
                        + "1.7976931348623157E308,\"NaN\",\"-Infinity\",\"Infinity\"]");
        assertArrayJson(
                new Float[] {
                    null,
                    -0.0f,
                    0.0f,
                    -Float.MIN_VALUE,
                    Float.MIN_VALUE,
                    -Float.MAX_VALUE,
                    Float.MAX_VALUE,
                    Float.NaN,
                    Float.NEGATIVE_INFINITY,
                    Float.POSITIVE_INFINITY
                },
                "[null,-0.0,0.0,-1.4E-45,1.4E-45,-3.4028235E38,3.4028235E38,"
                        + "\"NaN\",\"-Infinity\",\"Infinity\"]");
    }

    @Test
    void directBoxedLongAndBooleanArraysPreserveNullsAndValueTypes() throws Exception {
        assertArrayJson(
                new Long[] {null, Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE},
                "[null,\"-9223372036854775808\",\"-1\",\"0\",\"1\",\"9223372036854775807\"]");
        assertArrayJson(new Boolean[] {true, null, false}, "[true,null,false]");
    }

    @Test
    void emptyBoxedArraysRemainJsonArrays() throws Exception {
        for (Object array : new Object[] {new Double[0], new Float[0], new Long[0], new Boolean[0]})
            assertArrayJson(array, "[]");
    }

    static void assertArrayJson(Object array, String expectedJson) throws Exception {
        assertEquals(
                new ObjectMapper().readTree(expectedJson),
                MemoryBoundsTest.value(array),
                array.getClass().getName());
    }

    @Test
    void directPrimitiveWritersPreserveNumbersAndSpecialValues() throws Exception {
        Object[] arrays = {
            new double[] {
                -0.0, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN, Double.POSITIVE_INFINITY
            },
            new float[] {-0.0f, Float.MIN_VALUE, Float.MAX_VALUE, Float.NEGATIVE_INFINITY},
            new long[] {Long.MIN_VALUE, Long.MAX_VALUE},
            new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE},
            new byte[] {-128, 0, 127},
            new short[] {Short.MIN_VALUE, Short.MAX_VALUE},
            new boolean[] {true, false},
            new int[0],
            new double[][] {{-0.0, Double.NaN}, {1.0, 2.5}}
        };
        ObjectMapper mapper = new ObjectMapper();
        for (Object array : arrays) {
            String expected = mapper.writeValueAsString(NotificationJson.normalize(array));
            JsonNode encoded =
                    mapper.readTree(
                            NotificationJson.encode(
                                    MemoryBoundsTest.TAG, new DataValue(new Variant(array)), 4096));
            assertEquals(
                    mapper.readTree(expected), encoded.get("value"), array.getClass().getName());
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "1601-01-01T00:00:00.000000100Z",
                "1969-12-31T23:59:59.999999900Z",
                "1970-01-01T00:00:00Z",
                "2026-10-29T12:34:56.123456700Z"
            })
    void notificationTimesRetainUtcTickPrecisionAcrossTheUnixEpoch(String text) throws Exception {
        Instant instant = Instant.parse(text);
        DateTime date = new DateTime(instant);
        for (String format : Arrays.asList("ISO-8601", "Epoch Milliseconds")) {
            OutputSettings settings =
                    new OutputSettings(
                            "tag,value,sourceTimestamp,serverTimestamp",
                            "JSON Array",
                            format,
                            false,
                            0);
            JsonNode record =
                    new ObjectMapper()
                            .readTree(
                                    NotificationJson.encode(
                                            MemoryBoundsTest.TAG,
                                            new DataValue(
                                                    new Variant(date), StatusCode.GOOD, date, date),
                                            4096,
                                            settings));
            assertEquals(instant.toString(), record.get("value").asText());
            if (settings.epochMillis)
                assertEquals(instant.toEpochMilli(), record.get("sourceTimestamp").longValue());
            else assertEquals(instant.toString(), record.get("sourceTimestamp").asText());
            assertEquals(record.get("sourceTimestamp"), record.get("serverTimestamp"));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 255, 65535, 0x40000480L, 0x80170000L, 0xffffffffL, -1L})
    void statusHexAndQualityRemainUnchanged(long code) throws Exception {
        StatusCode status = new StatusCode(code);
        JsonNode record =
                new ObjectMapper()
                        .readTree(
                                NotificationJson.encode(
                                        MemoryBoundsTest.TAG,
                                        new DataValue(new Variant(2), status, null, null),
                                        1024));
        assertEquals(
                String.format(Locale.ROOT, "0x%08X", status.getValue()),
                record.get("statusCode").asText());
        assertEquals(
                status.isGood() ? "Good" : status.isBad() ? "Bad" : "Uncertain",
                record.get("quality").asText());
        List<String> fields = new ArrayList<>();
        record.fieldNames().forEachRemaining(fields::add);
        assertEquals(
                Arrays.asList(
                        "tag",
                        "nodeId",
                        "value",
                        "valueType",
                        "statusCode",
                        "quality",
                        "sourceTimestamp",
                        "receivedTimestamp"),
                fields);
        assertTrue(record.get("sourceTimestamp").isNull());
        assertNotNull(Instant.parse(record.get("receivedTimestamp").asText()));
    }

    @Test
    void diagnosticTimestampsKeepTheirMeaningAndJsonStringRepresentation() {
        ConnectionDiagnostics diagnostics = new ConnectionDiagnostics();
        assertNull(diagnostics.snapshot().get("lastNotificationReceivedAt"));
        diagnostics.keepAlive();
        diagnostics.notification(new DataValue(new Variant(1)));
        Map<String, Object> good = diagnostics.snapshot();
        assertEquals(good.get("lastNotificationReceivedAt"), good.get("lastGoodValueReceivedAt"));
        diagnostics.notification(
                new DataValue(new Variant(2), new StatusCode(0x80000000L), null, null));
        Map<String, Object> bad = diagnostics.snapshot();
        assertEquals(good.get("lastGoodValueReceivedAt"), bad.get("lastGoodValueReceivedAt"));
        assertEquals(2L, bad.get("notificationsReceived"));
        for (String key :
                Arrays.asList(
                        "lastKeepAliveReceivedAt",
                        "lastNotificationReceivedAt",
                        "lastGoodValueReceivedAt"))
            assertNotNull(Instant.parse((String) bad.get(key)));
    }
}
