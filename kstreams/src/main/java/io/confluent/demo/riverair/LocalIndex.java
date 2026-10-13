package io.confluent.demo.riverair;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Keeps secondary indexes of a table in the same task as the table, so a view such as "passengers on
 * RA417" needs no repartition topic, no extra sub-topology, and no extra hop. Each index stores the
 * row under a new key (for example RA417-20260928|P-0928-417-081) that range reads can scan by prefix.
 * Because an index holds only this task's rows, a range read asks every instance, which the query
 * server does anyway.
 *
 * <p>A second store remembers each row's current index keys, so an update or a tombstone removes the
 * old entries.
 */
final class LocalIndex<V> implements FixedKeyProcessor<String, V, Void> {

    /** One index: its store, and the index key for a row (null when the row isn't in this index). */
    record Index<V>(String store, BiFunction<String, V, String> key) {
    }

    private final String keysStore;
    private final List<Index<V>> indexes;
    private KeyValueStore<String, String[]> currentKeys;
    private final List<KeyValueStore<String, V>> stores = new ArrayList<>();

    LocalIndex(String keysStore, List<Index<V>> indexes) {
        this.keysStore = keysStore;
        this.indexes = indexes;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Void> context) {
        currentKeys = context.getStateStore(keysStore);
        for (Index<V> index : indexes) {
            stores.add(context.getStateStore(index.store()));
        }
    }

    @Override
    public void process(FixedKeyRecord<String, V> record) {
        String[] previous = currentKeys.get(record.key());
        String[] next = new String[indexes.size()];
        boolean indexed = false;
        for (int i = 0; i < indexes.size(); i++) {
            next[i] = record.value() == null ? null : indexes.get(i).key().apply(record.key(), record.value());
            String old = previous == null ? null : previous[i];
            if (old != null && !Objects.equals(old, next[i])) stores.get(i).delete(old);
            if (next[i] != null) {
                stores.get(i).put(next[i], record.value());
                indexed = true;
            }
        }
        if (indexed) {
            currentKeys.put(record.key(), next);
        } else if (previous != null) {
            currentKeys.delete(record.key());
        }
    }
}
