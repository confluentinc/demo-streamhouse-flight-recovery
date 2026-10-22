package io.confluent.demo.riverair;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The River Air operations view, computed straight from the source topics the generator writes.
 * It applies the same rules as Flink's passenger_state and flight_impact, then keeps the views the
 * dashboard reads (flights by scheduled time, passengers by flight, booked offers) as local indexes
 * in the same tasks, so they add no repartition topics.
 *
 * <p>Four sub-topologies: flights (with the at-risk count and flight impact), itineraries (with both
 * foreign-key joins and passenger state), hotels, and offers. The only internal topics between them
 * are the two foreign-key joins' and the at-risk count's.
 */
public final class OpsTopology {
    public static final String FLIGHT_STATUS = "flight_status";
    public static final String PASSENGER_ITINERARIES = "passenger_itineraries";
    public static final String HOTEL_INVENTORY = "hotel_inventory";
    public static final String PASSENGER_RECOMMENDATIONS = "passenger_recommendations";

    public static final String FLIGHTS_STORE = "flights";
    public static final String PASSENGER_STATE_STORE = "passenger-state";
    public static final String AT_RISK_STORE = "at-risk-by-flight";
    public static final String FLIGHT_IMPACT_STORE = "flight-impact";
    public static final String FLIGHTS_BY_TIME_STORE = "flights-by-scheduled-time";
    public static final String PASSENGERS_BY_INBOUND_STORE = "passengers-by-inbound-flight";
    public static final String PASSENGERS_BY_CONNECTING_STORE = "passengers-by-connecting-flight";
    public static final String HOTELS_STORE = "hotels";
    public static final String OFFERS_STORE = "offers";
    public static final String BOOKED_BY_TIME_STORE = "booked-offers-by-time";

    /** Separates the two parts of a composite store key, for example RA417-20260928|P-0928-417-081. */
    public static final String SEP = "|";

    private OpsTopology() {
    }

    public static Topology build(Map<String, ?> schemaRegistryConfig) {
        Serde<String> keys = Serdes.String();
        Serde<GenericRecord> avro = new GenericAvroSerde();
        avro.configure(schemaRegistryConfig, false);
        Serde<PassengerState> passengerSerde = JsonSerde.of(PassengerState.class);
        Serde<FlightImpact> impactSerde = JsonSerde.of(FlightImpact.class);
        Serde<Offer> offerSerde = JsonSerde.of(Offer.class);

        StreamsBuilder builder = new StreamsBuilder();

        // Source tables. The topics are compacted upserts keyed by flight, passenger, hotel, and offer. Repeated,
        // unchanged rows are dropped here, before they fan out through the joins (see DropUnchanged).
        KTable<String, Flight> flights = changesOnly(builder, FLIGHT_STATUS, "flight-status", avro,
                Flight::from, JsonSerde.of(Flight.class), FLIGHTS_STORE);
        KTable<String, Itinerary> itineraries = changesOnly(builder, PASSENGER_ITINERARIES, "passenger-itineraries", avro,
                Itinerary::from, JsonSerde.of(Itinerary.class), "itineraries");
        changesOnly(builder, HOTEL_INVENTORY, "hotel-inventory", avro, Hotel::from, JsonSerde.of(Hotel.class), HOTELS_STORE);
        KTable<String, Offer> offers = changesOnly(builder, PASSENGER_RECOMMENDATIONS, "passenger-recommendations", avro,
                Offer::from, offerSerde, OFFERS_STORE);

        // passenger_state: itinerary JOIN inbound flight LEFT JOIN connecting flight, then the risk rule.
        KTable<String, PassengerState> passengers = itineraries
                .join(flights, Itinerary::inboundFlightId, InboundLeg::new,
                        TableJoined.as("join-inbound-flight"),
                        Materialized.<String, InboundLeg, KeyValueStore<Bytes, byte[]>>as("inbound-legs")
                                .withKeySerde(keys).withValueSerde(JsonSerde.of(InboundLeg.class)))
                .leftJoin(flights, leg -> leg.itinerary().connectingFlightId(), PassengerState::of,
                        TableJoined.as("join-connecting-flight"),
                        Materialized.<String, PassengerState, KeyValueStore<Bytes, byte[]>>as(PASSENGER_STATE_STORE)
                                .withKeySerde(keys).withValueSerde(passengerSerde));

        // flight_impact: HIGH passengers counted per inbound flight, joined back to every flight. Only a change
        // of risk crosses the repartition: most flight updates move connection minutes without flipping it.
        Serde<FlightRisk> riskSerde = JsonSerde.of(FlightRisk.class);
        KTable<String, FlightRisk> risks = changesOnly(builder,
                passengers.toStream(Named.as("passenger-changes")).mapValues(FlightRisk::of, Named.as("passenger-risk")),
                "passenger-risk", riskSerde, "passenger-risks");
        KTable<String, Integer> atRisk = risks
                .groupBy((passengerId, r) -> KeyValue.pair(r.inboundFlightId(), r),
                        Grouped.with("at-risk", keys, riskSerde))
                .aggregate(() -> 0,
                        (flightId, r, count) -> count + (r.high() ? 1 : 0),
                        (flightId, r, count) -> count - (r.high() ? 1 : 0),
                        Named.as("count-at-risk"),
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(AT_RISK_STORE)
                                .withKeySerde(keys).withValueSerde(Serdes.Integer()));

        KTable<String, FlightImpact> impact = flights
                .leftJoin(atRisk, AtRiskFlight::new, Named.as("join-at-risk"))
                .mapValues((flightId, f) -> FlightImpact.of(flightId, f.flight(), f.atRisk()), Named.as("flight-impact"),
                        Materialized.<String, FlightImpact, KeyValueStore<Bytes, byte[]>>as(FLIGHT_IMPACT_STORE)
                                .withKeySerde(keys).withValueSerde(impactSerde));

        // The dashboard's views, as local indexes: the "18 hours back, 6 ahead" window is one range read, an
        // arrival lists its inbound passengers and a departure its connecting ones, and the counts bar
        // counts booked offers since a time.
        localIndex(builder, impact, "flight-index", impactSerde,
                new LocalIndex.Index<>(FLIGHTS_BY_TIME_STORE, (id, f) -> sortableTime(f.scheduledTime()) + SEP + id));
        localIndex(builder, passengers, "passenger-index", passengerSerde,
                new LocalIndex.Index<>(PASSENGERS_BY_INBOUND_STORE, (id, p) -> p.inboundFlightId() + SEP + id),
                new LocalIndex.Index<>(PASSENGERS_BY_CONNECTING_STORE,
                        (id, p) -> p.connectingFlightId() == null ? null : p.connectingFlightId() + SEP + id));
        localIndex(builder, offers, "offer-index", offerSerde,
                new LocalIndex.Index<>(BOOKED_BY_TIME_STORE,
                        (id, o) -> o.isBooked() ? sortableTime(o.recommendedAt()) + SEP + id : null));

        return builder.build();
    }

    /** Epoch millis padded to 13 digits, so keys sort by time. */
    public static String sortableTime(Long millis) {
        return String.format("%013d", millis == null ? 0L : millis);
    }

    /** A flight with its HIGH-risk count; null when no passenger on it has been seen yet. */
    record AtRiskFlight(Flight flight, Integer atRisk) {
    }

    /** Reads a compacted topic as a table, parsing each row and skipping updates that change nothing. */
    private static <V> KTable<String, V> changesOnly(StreamsBuilder builder, String topic, String name,
                                                     Serde<GenericRecord> avro,
                                                     java.util.function.Function<GenericRecord, V> parse,
                                                     Serde<V> valueSerde, String store) {
        KStream<String, V> rows = builder.stream(topic, Consumed.with(Serdes.String(), avro).withName("read-" + name))
                .mapValues(value -> value == null ? null : parse.apply(value), Named.as("parse-" + name));
        return changesOnly(builder, rows, name, valueSerde, store);
    }

    /** Turns a stream of upserts into a table, skipping updates that change nothing (see DropUnchanged). */
    private static <V> KTable<String, V> changesOnly(StreamsBuilder builder, KStream<String, V> rows, String name,
                                                     Serde<V> valueSerde, String store) {
        String lastSeen = name + "-last-seen";
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(lastSeen),
                Serdes.String(), valueSerde));
        return rows
                .processValues(() -> new DropUnchanged<V>(lastSeen), Named.as("drop-unchanged-" + name), lastSeen)
                .toTable(Named.as(name + "-table"),
                        Materialized.<String, V, KeyValueStore<Bytes, byte[]>>as(store)
                                .withKeySerde(Serdes.String()).withValueSerde(valueSerde));
    }

    /** Indexes a table's rows in stores in the same task (see LocalIndex), with no repartition topic. */
    @SafeVarargs
    private static <V> void localIndex(StreamsBuilder builder, KTable<String, V> table, String name,
                                       Serde<V> valueSerde, LocalIndex.Index<V>... indexes) {
        String keysStore = name + "-keys";
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(keysStore),
                Serdes.String(), JsonSerde.of(String[].class)));
        List<String> stores = new ArrayList<>(List.of(keysStore));
        for (LocalIndex.Index<V> index : indexes) {
            builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(index.store()),
                    Serdes.String(), valueSerde));
            stores.add(index.store());
        }
        table.toStream(Named.as(name + "-changes"))
                .processValues(() -> new LocalIndex<>(keysStore, List.of(indexes)), Named.as(name),
                        stores.toArray(String[]::new));
    }
}
