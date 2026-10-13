package io.confluent.demo.riverair;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/** One row of the hotel_inventory topic, keyed by hotel name. */
public record Hotel(Integer availableRooms, BigDecimal nightlyRate) {

    static Hotel from(GenericRecord value) {
        Object rooms = value.get("available_rooms");
        return new Hotel(rooms == null ? null : ((Number) rooms).intValue(), decimal(value, "nightly_rate"));
    }

    /** Flink writes DECIMAL as Avro bytes with a decimal logical type. */
    static BigDecimal decimal(GenericRecord value, String field) {
        Object raw = value.get(field);
        if (raw == null) return null;
        if (raw instanceof BigDecimal d) return d;
        Schema schema = value.getSchema().getField(field).schema();
        if (schema.getType() == Schema.Type.UNION) {
            schema = schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst().orElseThrow();
        }
        int scale = ((LogicalTypes.Decimal) schema.getLogicalType()).getScale();
        ByteBuffer buffer = ((ByteBuffer) raw).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }
}
