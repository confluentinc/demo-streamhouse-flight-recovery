package io.confluent.demo.riverair;

import org.apache.avro.generic.GenericRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/** One row of the flight_status topic. Times are epoch millis, UTC. */
public record Flight(String origin, String destination, Long scheduledTime, Long estimatedTime, String status) {

    static Flight from(GenericRecord value) {
        return new Flight(
                text(value, "origin"),
                text(value, "destination"),
                millis(value, "scheduled_time"),
                millis(value, "estimated_time"),
                text(value, "status"));
    }

    static String text(GenericRecord value, String field) {
        Object raw = value.get(field);
        return raw == null ? null : raw.toString();
    }

    /** Flink writes TIMESTAMP(3) as a long; accept the logical-type objects too in case a deserializer converts them. */
    static Long millis(GenericRecord value, String field) {
        Object raw = value.get(field);
        if (raw == null) return null;
        if (raw instanceof Long l) return l;
        if (raw instanceof Instant i) return i.toEpochMilli();
        if (raw instanceof LocalDateTime t) return t.toInstant(ZoneOffset.UTC).toEpochMilli();
        throw new IllegalArgumentException(field + " has unexpected type " + raw.getClass());
    }
}
