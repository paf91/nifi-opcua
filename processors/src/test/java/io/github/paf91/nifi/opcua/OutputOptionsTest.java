/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.*;

import org.apache.nifi.util.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.time.Instant;
import java.util.*;

class OutputOptionsTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TagDefinition TAG = MemoryBoundsTest.TAG;

    static OutputSettings output(
            String fields, String format, String timestamps, boolean routing, int delay) {
        return new OutputSettings(fields, format, timestamps, routing, delay);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "value",
                "tag",
                "tag,value,typo",
                "tag,value,",
                "tag,tag,value",
                "tag,Value",
                "configuredNodeId,value"
            })
    void rejectsInvalidFieldLists(String fields) {
        TestRunner r = ProcessorTest.configured(new ProcessorTest.OfflineProcessor());
        r.setProperty(ConsumeOPCUA.OUTPUT_FIELDS, fields);
        r.assertNotValid();
    }

    @ParameterizedTest
    @EnumSource(OutputSettings.Field.class)
    void optionalFieldsAreSelectedIndividually(OutputSettings.Field field) throws Exception {
        String names =
                "tag,value"
                        + (field == OutputSettings.Field.tag || field == OutputSettings.Field.value
                                ? ""
                                : "," + field);
        TagDefinition tag =
                TagDefinition.parse("sensor", "nsu=urn:test;s=Value", 100, 10)
                        .resolved(new NodeId(3, "Value"));
        OutputSettings settings = output(names, "JSON Array", "ISO-8601", false, 0);
        JsonNode json =
                JSON.readTree(
                        NotificationJson.encode(
                                tag, new DataValue(new Variant(2)), 4096, settings));
        Set<String> keys = new HashSet<>();
        json.fieldNames().forEachRemaining(keys::add);
        assertEquals(new HashSet<>(Arrays.asList(names.split(","))), keys);
        assertEquals(2, json.get("value").intValue());
    }

    @Test
    void defaultFieldsKeepLegacySchemaIncludingConditionalNamespaceFields() throws Exception {
        JsonNode numeric =
                JSON.readTree(NotificationJson.encode(TAG, new DataValue(new Variant(1))));
        assertEquals(8, numeric.size());
        assertFalse(numeric.has("namespaceUri"));
        assertFalse(numeric.has("serverTimestamp"));
        TagDefinition uri =
                TagDefinition.parse("uri", "nsu=urn:test;s=Value", 100, 10)
                        .resolved(new NodeId(9, "Value"));
        JsonNode namespaced =
                JSON.readTree(NotificationJson.encode(uri, new DataValue(new Variant(1))));
        assertEquals(10, namespaced.size());
        assertEquals("ns=9;s=Value", namespaced.get("nodeId").asText());
        assertEquals("nsu=urn:test;s=Value", namespaced.get("configuredNodeId").asText());
    }

    @Test
    void omittedNodeIdIsNotFormattedAndCachedIdsAreRevalidatedAgainstByteLimits() throws Exception {
        TagDefinition unresolved = TagDefinition.parse("short", "nsu=urn:test;s=Value", 100, 10);
        OutputSettings compact = output("tag,value", "JSON Array", "ISO-8601", false, 0);
        assertEquals(
                2,
                JSON.readTree(
                                NotificationJson.encode(
                                        unresolved, new DataValue(new Variant(1)), 100, compact))
                        .size());
        String text = TAG.nodeIdText(1024);
        assertSame(text, TAG.nodeIdText(1024));
        assertThrows(NotificationJson.RecordTooLargeException.class, () -> TAG.nodeIdText(1));
        assertEquals("ns=7;s=Memory", TAG.resolved(new NodeId(7, "Memory")).nodeIdText(1024));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ISO-8601", "Epoch Milliseconds"})
    void formatsAllNotificationTimestampsAndPreservesDateTimeValues(String format)
            throws Exception {
        Instant time = Instant.parse("1969-12-31T23:59:59.123456700Z");
        DateTime source = new DateTime(time);
        OutputSettings settings =
                output(
                        "tag,value,sourceTimestamp,serverTimestamp,receivedTimestamp",
                        "JSON Array",
                        format,
                        false,
                        0);
        long before = System.currentTimeMillis();
        JsonNode json =
                JSON.readTree(
                        NotificationJson.encode(
                                TAG,
                                new DataValue(new Variant(source), StatusCode.GOOD, source, source),
                                4096,
                                settings));
        assertEquals(time.toString(), json.get("value").asText());
        if (format.equals("ISO-8601")) {
            assertEquals(time.toString(), json.get("sourceTimestamp").asText());
            assertEquals(time.toString(), json.get("serverTimestamp").asText());
            assertTrue(
                    Instant.parse(json.get("receivedTimestamp").asText()).toEpochMilli() >= before);
        } else {
            assertEquals(-877L, json.get("sourceTimestamp").longValue());
            assertEquals(-877L, json.get("serverTimestamp").longValue());
            assertTrue(json.get("receivedTimestamp").longValue() >= before);
            assertTrue(json.get("receivedTimestamp").longValue() <= System.currentTimeMillis());
        }
        JsonNode missing =
                JSON.readTree(
                        NotificationJson.encode(
                                TAG,
                                new DataValue(
                                        new Variant(1), StatusCode.GOOD, null, new DateTime(0)),
                                4096,
                                settings));
        assertTrue(missing.get("sourceTimestamp").isNull());
        assertTrue(missing.get("serverTimestamp").isNull());
    }

    @Test
    void projectionKeepsStatusBitsNullsAndExactIntegersWithinByteLimit() throws Exception {
        OutputSettings compact =
                output(OutputSettings.COMPACT_FIELDS, "JSON Array", "ISO-8601", false, 0);
        DataValue value =
                new DataValue(new Variant(Long.MAX_VALUE), new StatusCode(0x00000480L), null, null);
        byte[] encoded = NotificationJson.encode(TAG, value, 4096, compact);
        JsonNode json = JSON.readTree(encoded);
        assertEquals("9223372036854775807", json.get("value").asText());
        assertEquals("0x00000480", json.get("statusCode").asText());
        assertTrue(json.get("sourceTimestamp").isNull());
        assertArrayEquals(encoded, NotificationJson.encode(TAG, value, encoded.length, compact));
        assertThrows(
                java.io.IOException.class,
                () -> NotificationJson.encode(TAG, value, encoded.length - 1, compact));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JSON Array", "NDJSON"})
    void writesSelectedFieldsWithCorrectFramingAttributesAndProvenance(String format)
            throws Exception {
        ConsumeOPCUA p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        r.setProperty(ConsumeOPCUA.OUTPUT_FORMAT, format);
        r.setProperty(ConsumeOPCUA.OUTPUT_FIELDS, "tag,value");
        r.setProperty(ConsumeOPCUA.RECORD_BYTES, "80");
        r.setProperty(ConsumeOPCUA.BATCH_BYTES, "82");
        p.start(r.getProcessContext());
        try {
            for (int i = 0; i < 5; i++) p.acceptForTest(TAG, new DataValue(new Variant("Ω\n" + i)));
            r.run(5, false, false);
            List<String> values = new ArrayList<>();
            for (MockFlowFile flow : r.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS)) {
                byte[] bytes = r.getContentAsByteArray(flow);
                assertTrue(bytes.length <= 82);
                flow.assertAttributeEquals(
                        "mime.type",
                        format.equals("NDJSON") ? "application/x-ndjson" : "application/json");
                flow.assertAttributeEquals("opcua.output.fields", "tag,value");
                flow.assertAttributeEquals("opcua.output.format", format);
                List<JsonNode> records = new ArrayList<>();
                if (format.equals("NDJSON")) {
                    assertEquals('\n', bytes[bytes.length - 1]);
                    for (String line :
                            new String(bytes, java.nio.charset.StandardCharsets.UTF_8).split("\n"))
                        records.add(JSON.readTree(line));
                } else JSON.readTree(bytes).forEach(records::add);
                assertEquals(records.size(), Integer.parseInt(flow.getAttribute("record.count")));
                for (JsonNode record : records) {
                    assertEquals(2, record.size());
                    values.add(record.get("value").asText());
                }
            }
            assertEquals(Arrays.asList("Ω\n0", "Ω\n1", "Ω\n2", "Ω\n3", "Ω\n4"), values);
            assertEquals(
                    r.getFlowFilesForRelationship(ConsumeOPCUA.SUCCESS).size(),
                    r.getProvenanceEvents().size());
        } finally {
            p.stop();
        }
    }

    @Test
    void qualityRoutingUsesOriginalStatusAndKeepsOrderAcrossHomogeneousBatches() throws Exception {
        ConsumeOPCUA p = new ProcessorTest.OfflineProcessor();
        TestRunner r = ProcessorTest.configured(p);
        assertFalse(p.getRelationships().contains(ConsumeOPCUA.BAD));
        r.setProperty(ConsumeOPCUA.QUALITY_ROUTING, "true");
        r.setProperty(ConsumeOPCUA.OUTPUT_FIELDS, "tag,value");
        assertTrue(p.getRelationships().contains(ConsumeOPCUA.BAD));
        p.start(r.getProcessContext());
        try {
            StatusCode[] statuses = {
                StatusCode.GOOD,
                new StatusCode(0x480L),
                StatusCode.BAD,
                StatusCode.UNCERTAIN,
                null,
                StatusCode.GOOD
            };
            for (int i = 0; i < statuses.length; i++)
                p.acceptForTest(TAG, new DataValue(new Variant(i), statuses[i], null, null));
            r.run(4, false, false);
            r.assertTransferCount(ConsumeOPCUA.SUCCESS, 2);
            r.assertTransferCount(ConsumeOPCUA.BAD, 1);
            r.assertTransferCount(ConsumeOPCUA.UNCERTAIN, 1);
            assertEquals(Arrays.asList(0, 1, 5), values(r, ConsumeOPCUA.SUCCESS));
            assertEquals(Collections.singletonList(2), values(r, ConsumeOPCUA.BAD));
            assertEquals(Arrays.asList(3, 4), values(r, ConsumeOPCUA.UNCERTAIN));
        } finally {
            p.stop();
        }
    }

    static List<Integer> values(TestRunner r, org.apache.nifi.processor.Relationship relationship)
            throws Exception {
        List<Integer> values = new ArrayList<>();
        for (MockFlowFile flow : r.getFlowFilesForRelationship(relationship)) {
            for (JsonNode record : JSON.readTree(r.getContentAsByteArray(flow)))
                values.add(record.get("value").intValue());
        }
        return values;
    }
}
