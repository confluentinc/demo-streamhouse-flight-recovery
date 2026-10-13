package io.confluent.demo.riverair;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OpsTopologyTest {
    private static final String SCOPE = "ops-topology-test";
    private static final Map<String, String> SR = Map.of("schema.registry.url", "mock://" + SCOPE);
    private static final Instant T = Instant.parse("2026-09-28T21:40:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, GenericRecord> flights;
    private TestInputTopic<String, GenericRecord> itineraries;
    private Schema flightSchema;
    private Schema itinerarySchema;

    @BeforeEach
    void setUp() throws IOException {
        flightSchema = schema("flight_status");
        itinerarySchema = schema("passenger_itineraries");
        Properties props = OpsApp.baseline();
        props.remove(StreamsConfig.GROUP_PROTOCOL_CONFIG); // TopologyTestDriver has no group
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // deterministic assertions
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(OpsTopology.build(SR), props);
        GenericAvroSerde avro = new GenericAvroSerde();
        avro.configure(SR, false);
        // Every record gets a later timestamp, so a store's timestamp shows whether an update reached it.
        flights = driver.createInputTopic(OpsTopology.FLIGHT_STATUS, new StringSerializer(), avro.serializer(),
                T, java.time.Duration.ofSeconds(1));
        itineraries = driver.createInputTopic(OpsTopology.PASSENGER_ITINERARIES, new StringSerializer(),
                avro.serializer(), T, java.time.Duration.ofSeconds(1));
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    void connectionUnder45MinutesIsHighAndCountsAgainstTheInboundFlight() {
        flight("RA417-20260928", "ORD", "SFO", 0, 0);
        flight("RA680-20260928", "SFO", "PHX", 60, 60);
        itinerary("P-1", "RA417-20260928", "RA680-20260928");
        itinerary("P-2", "RA417-20260928", null);

        assertEquals("OK", passenger("P-1").risk());
        assertEquals(60, passenger("P-1").connectionMinutes());
        assertEquals("PHX", passenger("P-1").finalDestination());
        assertEquals("NO_CONNECTION", passenger("P-2").risk());
        assertEquals("SFO", passenger("P-2").finalDestination());
        assertEquals(0, impact("RA417-20260928").affectedPassengers());

        // RA417 slips 16 minutes: 44 minutes to connect is HIGH. 45 exactly would still be OK.
        flight("RA417-20260928", "ORD", "SFO", 0, 16);
        assertEquals("HIGH", passenger("P-1").risk());
        assertEquals(44, passenger("P-1").connectionMinutes());
        assertEquals(1, impact("RA417-20260928").affectedPassengers());
        assertEquals(16, impact("RA417-20260928").delayMinutes());
        assertEquals(0, impact("RA680-20260928").affectedPassengers());

        // Both flights are in the scheduled-time index, RA417 (scheduled first) first.
        assertEquals(2, count(OpsTopology.FLIGHTS_BY_TIME_STORE, ""));
        assertEquals(1, ((FlightImpact) store(OpsTopology.FLIGHTS_BY_TIME_STORE)
                .get(OpsTopology.sortableTime(T.toEpochMilli()) + "|RA417-20260928")).affectedPassengers());
    }

    @Test
    void onlyRealChangesReachTheJoinsAndTheCount() {
        flight("RA417-20260928", "ORD", "SFO", 0, 0);
        flight("RA680-20260928", "SFO", "PHX", 60, 60);
        itinerary("P-1", "RA417-20260928", "RA680-20260928");
        long passengerAt = updatedAt(OpsTopology.PASSENGER_STATE_STORE, "P-1");
        long riskAt = updatedAt("passenger-risks", "P-1");

        flight("RA417-20260928", "ORD", "SFO", 0, 0);       // the once-a-minute repeat
        itinerary("P-1", "RA417-20260928", "RA680-20260928"); // a scene restart's republish
        assertEquals(passengerAt, updatedAt(OpsTopology.PASSENGER_STATE_STORE, "P-1"));

        flight("RA417-20260928", "ORD", "SFO", 0, 5);       // connection shrinks to 55 minutes: still OK
        assertEquals(55, passenger("P-1").connectionMinutes());
        assertEquals(riskAt, updatedAt("passenger-risks", "P-1"));  // nothing for the at-risk count to do

        flight("RA417-20260928", "ORD", "SFO", 0, 20);      // now HIGH: that reaches the count
        assertEquals("HIGH", passenger("P-1").risk());
        assertEquals(1, impact("RA417-20260928").affectedPassengers());
    }

    private long updatedAt(String store, String key) {
        return driver.<String, Object>getTimestampedKeyValueStore(store).get(key).timestamp();
    }

    @Test
    void exactly45MinutesIsOk() {
        flight("RA417-20260928", "ORD", "SFO", 0, 15);
        flight("RA680-20260928", "SFO", "PHX", 60, 60);
        itinerary("P-1", "RA417-20260928", "RA680-20260928");
        assertEquals(45, passenger("P-1").connectionMinutes());
        assertEquals("OK", passenger("P-1").risk());
    }

    @Test
    void connectingFlightNotSeenYetIsOkUntilItArrives() {
        flight("RA417-20260928", "ORD", "SFO", 0, 30);
        itinerary("P-1", "RA417-20260928", "RA680-20260928");
        assertEquals("OK", passenger("P-1").risk());
        assertNull(passenger("P-1").connectionMinutes());

        flight("RA680-20260928", "SFO", "PHX", 60, 60);
        assertEquals("HIGH", passenger("P-1").risk());
    }

    @Test
    void passengersAreIndexedByBothFlights() {
        flight("RA417-20260928", "ORD", "SFO", 0, 0);
        flight("RA680-20260928", "SFO", "PHX", 60, 60);
        itinerary("P-1", "RA417-20260928", "RA680-20260928");
        itinerary("P-2", "RA417-20260928", null);

        assertEquals(2, count(OpsTopology.PASSENGERS_BY_INBOUND_STORE, "RA417-20260928|"));
        assertEquals(1, count(OpsTopology.PASSENGERS_BY_CONNECTING_STORE, "RA680-20260928|"));
        assertEquals(2, count(OpsTopology.FLIGHTS_BY_TIME_STORE, ""));
    }

    @Test
    void tombstonesRemovePassengersAndFlights() {
        flight("RA417-20260928", "ORD", "SFO", 0, 16);
        flight("RA680-20260928", "SFO", "PHX", 60, 60);
        itinerary("P-1", "RA417-20260928", "RA680-20260928");
        assertEquals(1, impact("RA417-20260928").affectedPassengers());

        itineraries.pipeInput("P-1", null);
        assertNull(store(OpsTopology.PASSENGER_STATE_STORE).get("P-1"));
        assertEquals(0, impact("RA417-20260928").affectedPassengers());
        assertEquals(0, count(OpsTopology.PASSENGERS_BY_INBOUND_STORE, "RA417-20260928|"));

        flights.pipeInput("RA680-20260928", null);
        assertNull(store(OpsTopology.FLIGHT_IMPACT_STORE).get("RA680-20260928"));
        assertEquals(1, count(OpsTopology.FLIGHTS_BY_TIME_STORE, ""));
        assertEquals(0, count(OpsTopology.PASSENGERS_BY_CONNECTING_STORE, "RA680-20260928|"));
    }

    @Test
    void hotelsAndOffersAreQueryableByPassengerAndBookingTime() throws IOException {
        GenericAvroSerde avro = new GenericAvroSerde();
        avro.configure(SR, false);
        Schema hotelSchema = schema("hotel_inventory");
        Schema offerSchema = schema("passenger_recommendations");
        var hotels = driver.createInputTopic(OpsTopology.HOTEL_INVENTORY, new StringSerializer(), avro.serializer());
        var offers = driver.createInputTopic(OpsTopology.PASSENGER_RECOMMENDATIONS, new StringSerializer(), avro.serializer());

        GenericRecord hyatt = new GenericData.Record(hotelSchema);
        hyatt.put("available_rooms", 12);
        hyatt.put("nightly_rate", decimal(hotelSchema, "nightly_rate", "329.00"));
        hotels.pipeInput("Grand Hyatt at SFO", hyatt);
        Hotel stored = (Hotel) store(OpsTopology.HOTELS_STORE).get("Grand Hyatt at SFO");
        assertEquals(12, stored.availableRooms());
        assertEquals(new java.math.BigDecimal("329.00"), stored.nightlyRate());

        GenericRecord offer = new GenericData.Record(offerSchema);
        offer.put("passenger_id", "P-1");
        offer.put("recommended_flight_id", "RA605-20260929");
        offer.put("hotel_name", "Grand Hyatt at SFO");
        offer.put("status", "OFFERED");
        offer.put("hotel_cost", decimal(offerSchema, "hotel_cost", "329.00"));
        offer.put("impacted_at", T.toEpochMilli());
        offer.put("recommended_at", T.toEpochMilli() + 20_000);
        offers.pipeInput("P-1-O1", offer);
        assertEquals("OFFERED", ((Offer) store(OpsTopology.OFFERS_STORE).get("P-1-O1")).status());
        assertEquals(0, count(OpsTopology.BOOKED_BY_TIME_STORE, ""));

        offer.put("status", "BOOKED");
        offers.pipeInput("P-1-O1", offer);
        assertEquals(1, count(OpsTopology.BOOKED_BY_TIME_STORE, OpsTopology.sortableTime(T.toEpochMilli() + 20_000)));
        assertEquals("BOOKED", ((Offer) store(OpsTopology.OFFERS_STORE).get("P-1-O1")).status());

        offers.pipeInput("P-1-O1", null);  // a tombstone leaves the booked index too
        assertEquals(0, count(OpsTopology.BOOKED_BY_TIME_STORE, ""));
    }

    private static java.nio.ByteBuffer decimal(Schema record, String field, String value) {
        Schema bytes = record.getField(field).schema().getTypes().get(1);
        return new org.apache.avro.Conversions.DecimalConversion()
                .toBytes(new java.math.BigDecimal(value), bytes, bytes.getLogicalType());
    }

    private void flight(String id, String origin, String destination, int scheduledMinutes, int estimatedMinutes) {
        GenericRecord r = new GenericData.Record(flightSchema);
        r.put("origin", origin);
        r.put("destination", destination);
        r.put("scheduled_time", T.plusSeconds(scheduledMinutes * 60L).toEpochMilli());
        r.put("estimated_time", T.plusSeconds(estimatedMinutes * 60L).toEpochMilli());
        r.put("status", estimatedMinutes > scheduledMinutes ? "DELAYED" : "ON_TIME");
        flights.pipeInput(id, r);
    }

    private void itinerary(String passengerId, String inbound, String connecting) {
        GenericRecord r = new GenericData.Record(itinerarySchema);
        r.put("inbound_flight_id", inbound);
        r.put("inbound_seat", "19F");
        r.put("connecting_flight_id", connecting);
        r.put("connecting_seat", connecting == null ? null : "21C");
        itineraries.pipeInput(passengerId, r);
    }

    private PassengerState passenger(String id) {
        return (PassengerState) store(OpsTopology.PASSENGER_STATE_STORE).get(id);
    }

    private FlightImpact impact(String id) {
        return (FlightImpact) store(OpsTopology.FLIGHT_IMPACT_STORE).get(id);
    }

    private KeyValueStore<String, Object> store(String name) {
        return driver.getKeyValueStore(name);
    }

    private int count(String store, String prefix) {
        int n = 0;
        try (var it = store(store).prefixScan(prefix, Serdes.String().serializer())) {
            while (it.hasNext()) {
                it.next();
                n++;
            }
        }
        return n;
    }

    private static Schema schema(String topic) throws IOException {
        return new Schema.Parser().parse(Files.readString(Path.of("local/schemas/" + topic + ".avsc")));
    }
}
