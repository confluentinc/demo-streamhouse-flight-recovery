package io.confluent.demo.riverair;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

/**
 * Forwards an update only when the value differs from the last one seen for that key.
 *
 * <p>Every active flight republishes its row once a minute, usually unchanged, and restarting the
 * scene republishes every itinerary as it was. Without this, each repeat fans out through the
 * foreign-key joins to every passenger on the flight and on through four re-keyed views, for no change.
 */
final class DropUnchanged<V> implements FixedKeyProcessor<String, V, V> {
    private final String storeName;
    private FixedKeyProcessorContext<String, V> context;
    private KeyValueStore<String, V> last;

    DropUnchanged(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, V> context) {
        this.context = context;
        this.last = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, V> record) {
        V previous = last.get(record.key());
        if (record.value() != null && Objects.equals(previous, record.value())) return;
        if (record.value() == null && previous == null) return;
        if (record.value() == null) {
            last.delete(record.key());
        } else {
            last.put(record.key(), record.value());
        }
        context.forward(record);
    }
}
