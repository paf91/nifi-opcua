/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;

import io.netty.buffer.*;

import org.eclipse.milo.opcua.stack.core.encoding.DefaultEncodingContext;
import org.eclipse.milo.opcua.stack.core.encoding.binary.OpcUaBinaryDecoder;
import org.eclipse.milo.opcua.stack.core.encoding.binary.OpcUaBinaryEncoder;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

class MatrixEncodingTest {
    @Test
    void realWireBoxedFloatingPointArraysRetainTypesAndJsonValues() throws Exception {
        assertBoxedRoundTrip(
                new Double[] {
                    null,
                    -0.0,
                    Double.MIN_VALUE,
                    -Double.MAX_VALUE,
                    Double.MAX_VALUE,
                    Double.NaN,
                    Double.NEGATIVE_INFINITY,
                    Double.POSITIVE_INFINITY
                },
                new Double[] {
                    0.0,
                    -0.0,
                    Double.MIN_VALUE,
                    -Double.MAX_VALUE,
                    Double.MAX_VALUE,
                    Double.NaN,
                    Double.NEGATIVE_INFINITY,
                    Double.POSITIVE_INFINITY
                },
                "[0.0,-0.0,4.9E-324,-1.7976931348623157E308,1.7976931348623157E308,"
                        + "\"NaN\",\"-Infinity\",\"Infinity\"]");
        assertBoxedRoundTrip(
                new Float[] {
                    null,
                    -0.0f,
                    Float.MIN_VALUE,
                    -Float.MAX_VALUE,
                    Float.MAX_VALUE,
                    Float.NaN,
                    Float.NEGATIVE_INFINITY,
                    Float.POSITIVE_INFINITY
                },
                new Float[] {
                    0.0f,
                    -0.0f,
                    Float.MIN_VALUE,
                    -Float.MAX_VALUE,
                    Float.MAX_VALUE,
                    Float.NaN,
                    Float.NEGATIVE_INFINITY,
                    Float.POSITIVE_INFINITY
                },
                "[0.0,-0.0,1.4E-45,-3.4028235E38,3.4028235E38,"
                        + "\"NaN\",\"-Infinity\",\"Infinity\"]");
    }

    @Test
    void realWireBoxedLongAndBooleanArraysDefaultNullsAndRetainJsonValueTypes() throws Exception {
        assertBoxedRoundTrip(
                new Long[] {null, Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE},
                new Long[] {0L, Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE},
                "[\"0\",\"-9223372036854775808\",\"-1\",\"0\",\"1\",\"9223372036854775807\"]");
        assertBoxedRoundTrip(
                new Boolean[] {null, true, false},
                new Boolean[] {false, true, false},
                "[false,true,false]");
    }

    @Test
    void realWireEmptyBoxedArraysRetainTypesAndJsonArrayShape() throws Exception {
        for (Object[] array :
                new Object[][] {new Double[0], new Float[0], new Long[0], new Boolean[0]})
            assertBoxedRoundTrip(array, new Object[0], "[]");
    }

    private static void assertBoxedRoundTrip(Object[] input, Object[] expected, String expectedJson)
            throws Exception {
        ByteBuf wire = Unpooled.buffer();
        try {
            new OpcUaBinaryEncoder(DefaultEncodingContext.INSTANCE)
                    .setBuffer(wire)
                    .encodeVariant(new Variant(input));
            Variant decoded =
                    new OpcUaBinaryDecoder(DefaultEncodingContext.INSTANCE)
                            .setBuffer(wire)
                            .decodeVariant();
            assertEquals(0, wire.readableBytes());
            assertEquals(input.getClass(), decoded.getValue().getClass());
            assertArrayEquals(expected, (Object[]) decoded.getValue(), input.getClass().getName());
            EncodingCompatibilityTest.assertArrayJson(decoded.getValue(), expectedJson);
        } finally {
            wire.release();
        }
    }

    @Test
    void oversizedJsonUnitMetadataIsRejectedBeforeDecoding() throws Exception {
        TagMetadataTest.assertRejectedExtension(
                ExtensionObject.of(
                        "x".repeat(16385),
                        org.eclipse.milo.opcua.stack.core.Identifiers.EUInformation));
    }

    @Test
    void streamsWideNarrowEmptyNullAndThreeDimensionalMatrices() throws Exception {
        ObjectMapper json = new ObjectMapper();
        assertEquals(
                json.readTree("[[1,2,3,4]]"),
                MemoryBoundsTest.value(new Matrix(new int[] {1, 2, 3, 4}, new int[] {1, 4})));
        assertEquals(
                json.readTree("[[1],[2],[3],[4]]"),
                MemoryBoundsTest.value(new Matrix(new int[] {1, 2, 3, 4}, new int[] {4, 1})));
        assertEquals(
                json.readTree("[[[1,2]],[[3,4]]]"),
                MemoryBoundsTest.value(new Matrix(new int[] {1, 2, 3, 4}, new int[] {2, 1, 2})));
        assertEquals(
                json.readTree("[[],[]]"),
                MemoryBoundsTest.value(new Matrix(new int[0], new int[] {2, 0, 3})));
        assertEquals(
                json.readTree("[]"),
                MemoryBoundsTest.value(new Matrix(new int[0], new int[] {0, 3})));
        assertTrue(MemoryBoundsTest.value(Matrix.ofNull()).isNull());
        assertEquals(
                json.readTree("[[\"9223372036854775807\"]]"),
                MemoryBoundsTest.value(new Matrix(new long[] {Long.MAX_VALUE}, new int[] {1, 1})));
        assertEquals(
                json.readTree("[[\"AQID\",null]]"),
                MemoryBoundsTest.value(
                        new Matrix(
                                new ByteString[] {
                                    new ByteString(new byte[] {1, 2, 3}), new ByteString(null)
                                },
                                new int[] {1, 2})));
    }

    @Test
    void oversizedMatrixDoesNotAllocateNestedArrays() throws Exception {
        MemoryBoundsTest.value(new Matrix(new int[] {1, 2}, new int[] {2, 1}));
        Matrix matrix = new Matrix(new int[1000000], new int[] {1000000, 1});
        long before = MemoryBoundsTest.allocatedBytes();
        MemoryBoundsTest.assertOversized(matrix, 1024);
        long allocation = MemoryBoundsTest.allocatedBytes() - before;
        assertTrue(allocation < 512 * 1024, "Matrix allocation: " + allocation);
        System.out.println("MEMORY bounded-matrix allocatedBytes=" + allocation);
    }

    @Test
    void malformedAndExcessivelyDeepDimensionsFailBeforeTraversal() {
        int[] deep = new int[256];
        Arrays.fill(deep, 1);
        for (int[] dimensions : new int[][] {{2, 2}, {-1, 1}, {Integer.MAX_VALUE, 2}, deep}) {
            Matrix matrix = new Matrix(new int[] {1}, dimensions);
            assertThrows(IOException.class, () -> MemoryBoundsTest.value(matrix));
        }
    }

    @Test
    void jsonExtensionBodiesRemainQuotedAndRetainTheirDataTypeId() throws Exception {
        String body = "{\"custom\":[1,2],\"text\":\"Ω\"}";
        JsonNode value = MemoryBoundsTest.value(ExtensionObject.of(body, new NodeId(2, 999)));
        assertEquals("opcua-json", value.get("encoding").asText());
        assertEquals("ns=2;i=999", value.get("typeId").asText());
        assertEquals(body, value.get("body").asText());
        assertTrue(
                MemoryBoundsTest.value(ExtensionObject.of((String) null, new NodeId(2, 999)))
                        .get("body")
                        .isNull());
        MemoryBoundsTest.assertOversized(
                ExtensionObject.of("x".repeat(100000), new NodeId(2, 999)), 1024);
    }

    @Test
    void realWireMatrixRetainsItsNumericArrayShape() throws Exception {
        // OPC UA Binary Variant: Int32, array + dimensions, 4 elements, 2x2.
        ByteBuf wire = Unpooled.buffer();
        try {
            wire.writeByte(0xC6).writeIntLE(4);
            for (int i = 1; i <= 4; i++) wire.writeIntLE(i);
            wire.writeIntLE(2).writeIntLE(2).writeIntLE(2);
            Variant decoded =
                    new OpcUaBinaryDecoder(DefaultEncodingContext.INSTANCE)
                            .setBuffer(wire)
                            .decodeVariant();
            assertEquals(0, wire.readableBytes());
            JsonNode record =
                    new ObjectMapper()
                            .readTree(
                                    NotificationJson.encode(
                                            TagDefinition.parse("matrix", "ns=2;s=Matrix", 100, 10),
                                            new DataValue(decoded)));
            assertEquals(
                    new ObjectMapper().readTree("[[1,2],[3,4]]"),
                    record.get("value"),
                    "An OPC UA numeric matrix must remain a numeric JSON array");
        } finally {
            wire.release();
        }
    }
}
