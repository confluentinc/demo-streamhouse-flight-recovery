package io.confluent.demo.riverair;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.io.UncheckedIOException;

/** JSON serde for the app's own state stores and internal topics. */
final class JsonSerde {
    static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonSerde() {
    }

    static <T> Serde<T> of(Class<T> type) {
        return Serdes.serdeFrom(
                (topic, value) -> {
                    if (value == null) return null;
                    try {
                        return MAPPER.writeValueAsBytes(value);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                (topic, bytes) -> {
                    if (bytes == null) return null;
                    try {
                        return MAPPER.readValue(bytes, type);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }
}
