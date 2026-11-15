/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;

import io.netty.buffer.*;

import org.eclipse.milo.opcua.stack.core.channel.EncodingLimits;
import org.eclipse.milo.opcua.stack.core.serialization.OpcUaBinaryStreamDecoder;
import org.eclipse.milo.opcua.stack.core.serialization.OpcUaBinaryStreamEncoder;
import org.eclipse.milo.opcua.stack.core.serialization.SerializationContext;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.junit.jupiter.api.Test;

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
            SerializationContext context = mock(SerializationContext.class);
            when(context.getEncodingLimits()).thenReturn(EncodingLimits.DEFAULT);
            new OpcUaBinaryStreamEncoder(context).setBuffer(wire).writeVariant(new Variant(input));
            Variant decoded = new OpcUaBinaryStreamDecoder(context).setBuffer(wire).readVariant();
            assertEquals(0, wire.readableBytes());
            assertEquals(input.getClass(), decoded.getValue().getClass());
            assertArrayEquals(expected, (Object[]) decoded.getValue(), input.getClass().getName());
            EncodingCompatibilityTest.assertArrayJson(decoded.getValue(), expectedJson);
        } finally {
            wire.release();
        }
    }

    @Test
    void realWireMatrixRetainsItsNumericArrayShape() throws Exception {
        // OPC UA Binary Variant: Int32, array + dimensions, 4 elements, 2x2.
        ByteBuf wire = Unpooled.buffer();
        try {
            wire.writeByte(0xC6).writeIntLE(4);
            for (int i = 1; i <= 4; i++) wire.writeIntLE(i);
            wire.writeIntLE(2).writeIntLE(2).writeIntLE(2);
            SerializationContext context = mock(SerializationContext.class, RETURNS_DEEP_STUBS);
            when(context.getEncodingLimits().getMaxRecursionDepth()).thenReturn(128);
            when(context.getEncodingLimits().getMaxMessageSize()).thenReturn(65536);
            Variant decoded = new OpcUaBinaryStreamDecoder(context).setBuffer(wire).readVariant();
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
