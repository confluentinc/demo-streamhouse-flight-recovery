package io.confluent.demo.riverair;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Serves the operations view over HTTP from the app's state stores.
 *
 * <p>The app runs as several instances that share the work: each owns some of the tasks, and so
 * some of each store. A key lookup goes to the instance that owns the key (found with
 * {@link KafkaStreams#queryMetadataForKey}); a range read asks every instance that hosts the
 * store and merges the answers. Instances talk to each other over /internal, which reads only
 * local state. Every /api response says which instances answered (servedBy), how long it took
 * (queryMicros), and when (servedAt).
 */
final class QueryServer {
    private static final Logger log = LoggerFactory.getLogger(QueryServer.class);
    private static final String HUB = "SFO";
    private static final Map<String, Class<?>> VALUE_TYPES = Map.of(
            OpsTopology.FLIGHT_IMPACT_STORE, FlightImpact.class,
            OpsTopology.PASSENGER_STATE_STORE, PassengerState.class,
            OpsTopology.PASSENGERS_BY_INBOUND_STORE, PassengerState.class,
            OpsTopology.PASSENGERS_BY_CONNECTING_STORE, PassengerState.class,
            OpsTopology.FLIGHTS_BY_TIME_STORE, FlightImpact.class,
            OpsTopology.HOTELS_STORE, Hotel.class,
            OpsTopology.OFFERS_STORE, Offer.class,
            OpsTopology.BOOKED_BY_TIME_STORE, Offer.class);

    private final KafkaStreams streams;
    private final HostInfo self;
    private final HttpServer server;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final boolean useIqV2;
    private final Map<String, Integer> subtopologyOf = new HashMap<>();
    private final Map<String, Integer> partitionCounts = new ConcurrentHashMap<>();
    private final Properties clientConfig;
    private Admin admin;

    /**
     * @param self    this instance's application.server
     * @param useIqV2 read local stores with IQv2 (KafkaStreams#query) or IQv1 (KafkaStreams#store). IQv2 through
     *                Kafka Streams 4.4.0 can deadlock the stream thread (KAFKA-19629, fixed on trunk for 4.5): a
     *                query locks the store's Position and then the store, while a write takes them in the
     *                opposite order. IQv1 takes only the store lock.
     */
    QueryServer(KafkaStreams streams, HostInfo self, boolean useIqV2, TopologyDescription topology,
                Properties clientConfig) throws IOException {
        this.streams = streams;
        this.self = self;
        this.useIqV2 = useIqV2;
        this.clientConfig = clientConfig;
        for (TopologyDescription.Subtopology sub : topology.subtopologies()) {
            for (TopologyDescription.Node node : sub.nodes()) {
                if (node instanceof TopologyDescription.Processor processor) {
                    processor.stores().forEach(store -> subtopologyOf.put(store, sub.id()));
                }
            }
        }
        // Loopback only: the instances and the dashboard all run on this machine, and nothing else should reach them.
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), self.port()), 0);
        server.setExecutor(Executors.newFixedThreadPool(16));
        server.createContext("/api/health", ex -> respond(ex, false, this::health));
        server.createContext("/api/flights", ex -> respond(ex, true, () -> flights(ex)));
        server.createContext("/api/passengers/", ex -> respond(ex, true, () -> passengers(ex)));
        server.createContext("/api/hotels", ex -> respond(ex, true, this::hotels));
        server.createContext("/api/offers/booked", ex -> respond(ex, true, () -> booked(ex)));
        server.createContext("/internal/get", ex -> respond(ex, true, () -> {
            Map<String, String> q = params(ex);
            return Map.of("value", nullable(localGet(q.get("store"), q.get("key"), Integer.parseInt(q.get("partition")))));
        }));
        server.createContext("/internal/range", ex -> respond(ex, true, () -> {
            Map<String, String> q = params(ex);
            LocalRows local = localRange(q.get("store"), q.get("from"), q.get("to"));
            List<Map<String, Object>> rows = new ArrayList<>();
            for (KeyValue<String, Object> kv : local.rows()) {
                rows.add(Map.of("key", kv.key, "value", kv.value));
            }
            return Map.of("rows", rows, "partitions", local.partitions());
        }));
    }

    void start() {
        server.start();
        log.info("Serving {} queries on http://{}:{}/api", useIqV2 ? "IQv2" : "IQv1", self.host(), self.port());
    }

    void stop() {
        server.stop(0);
        if (admin != null) admin.close(Duration.ofSeconds(5));
    }

    private Object health() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instance", self.host() + ":" + self.port());
        body.put("state", streams.state().toString());
        List<String> tasks = new ArrayList<>();
        streams.metadataForLocalThreads().forEach(t -> t.activeTasks().forEach(task -> tasks.add(task.taskId().toString())));
        tasks.sort(null);
        body.put("activeTasks", tasks);
        return body;
    }

    /**
     * /api/flights?from=&to= (epoch millis, scheduled time) lists flights in a window;
     * /api/flights/{id} is one flight with its passengers; /api/flights/{id}/passengers?leg=inbound|connecting
     * lists the passengers on one leg.
     */
    private Object flights(HttpExchange ex) {
        String[] parts = ex.getRequestURI().getPath().split("/");
        if (parts.length == 3) {
            Map<String, String> q = query(ex);
            Answer answer = new Answer();
            answer.body.put("flights", answer.range(OpsTopology.FLIGHTS_BY_TIME_STORE,
                    OpsTopology.sortableTime(Long.parseLong(q.get("from"))),
                    OpsTopology.sortableTime(Long.parseLong(q.get("to"))), FlightImpact.class).stream()
                    .map(kv -> kv.value).toList());
            return answer.done();
        }
        if (parts.length == 5 && parts[4].equals("passengers")) {
            boolean inbound = !"connecting".equals(query(ex).get("leg"));
            Answer answer = new Answer();
            answer.body.put("passengers", passengersOn(answer, parts[3], inbound));
            return answer.done();
        }
        return flight(parts[3]);
    }

    /** /api/passengers/{id} or /api/passengers/{id}/offers */
    private Object passengers(HttpExchange ex) {
        String[] parts = ex.getRequestURI().getPath().split("/");
        if (parts.length == 5 && parts[4].equals("offers")) {
            // Every passenger's offers are {passenger}-O1 and -O2, so two key lookups find them.
            Answer answer = new Answer();
            List<Map<String, Object>> offers = new ArrayList<>();
            for (String offerId : List.of(parts[3] + "-O1", parts[3] + "-O2")) {
                Offer offer = answer.get(OpsTopology.OFFERS_STORE, offerId, Offer.class);
                if (offer != null) offers.add(withId("offerId", offerId, offer));
            }
            answer.body.put("offers", offers);
            return answer.done();
        }
        return passenger(parts[3]);
    }

    private Object hotels() {
        Answer answer = new Answer();
        List<Map<String, Object>> hotels = new ArrayList<>();
        for (KeyValue<String, Hotel> row : answer.range(OpsTopology.HOTELS_STORE, "", "\uffff", Hotel.class)) {
            hotels.add(withId("hotelName", row.key, row.value));
        }
        answer.body.put("hotels", hotels);
        return answer.done();
    }

    /** /api/offers/booked?since= (epoch millis): booked offers recommended since then. */
    private Object booked(HttpExchange ex) {
        long since = Long.parseLong(query(ex).get("since"));
        Answer answer = new Answer();
        List<Map<String, Object>> offers = new ArrayList<>();
        for (KeyValue<String, Offer> row : answer.range(OpsTopology.BOOKED_BY_TIME_STORE,
                OpsTopology.sortableTime(since), "\uffff", Offer.class)) {
            offers.add(withId("offerId", afterSep(row.key), row.value));
        }
        answer.body.put("offers", offers);
        return answer.done();
    }

    private List<Map<String, Object>> passengersOn(Answer answer, String flightId, boolean inbound) {
        String store = inbound ? OpsTopology.PASSENGERS_BY_INBOUND_STORE : OpsTopology.PASSENGERS_BY_CONNECTING_STORE;
        List<Map<String, Object>> passengers = new ArrayList<>();
        for (KeyValue<String, PassengerState> row : answer.range(store, flightId + OpsTopology.SEP, PassengerState.class)) {
            passengers.add(withId("passengerId", afterSep(row.key), row.value));
        }
        passengers.sort(Comparator.comparing((Map<String, Object> p) -> !"HIGH".equals(p.get("risk")))
                .thenComparing(p -> (String) p.get("passengerId")));
        return passengers;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> withId(String idField, String id, Object value) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(idField, id);
        row.putAll(JsonSerde.MAPPER.convertValue(value, LinkedHashMap.class));
        return row;
    }

    private static String afterSep(String key) {
        return key.substring(key.indexOf(OpsTopology.SEP) + 1);
    }

    /** One flight and its passengers: an arrival lists its inbound passengers, a departure its connecting ones. */
    private Object flight(String flightId) {
        Answer answer = new Answer();
        FlightImpact flight = answer.get(OpsTopology.FLIGHT_IMPACT_STORE, flightId, FlightImpact.class);
        if (flight == null) throw new NotFound("no flight " + flightId);
        answer.body.put("flight", flight);
        answer.body.put("passengers", passengersOn(answer, flightId, HUB.equals(flight.destination())));
        return answer.done();
    }

    private Object passenger(String passengerId) {
        Answer answer = new Answer();
        PassengerState passenger = answer.get(OpsTopology.PASSENGER_STATE_STORE, passengerId, PassengerState.class);
        if (passenger == null) throw new NotFound("no passenger " + passengerId);
        answer.body.put("passengerId", passengerId);
        answer.body.put("passenger", passenger);
        return answer.done();
    }

    /** One /api response: routes each read to the right instances and records which ones answered. */
    private final class Answer {
        final Map<String, Object> body = new LinkedHashMap<>();
        final TreeSet<String> servedBy = new TreeSet<>();
        final long started = System.nanoTime();

        <V> V get(String store, String key, Class<V> type) {
            KeyQueryMetadata owner = streams.queryMetadataForKey(store, key, Serdes.String().serializer());
            HostInfo host = owner.activeHost();
            if (host.equals(HostInfo.unavailable())) throw new Unavailable(store + " has no active owner for " + key);
            servedBy.add(host.host() + ":" + host.port());
            if (host.equals(self)) return type.cast(localGet(store, key, owner.partition()));
            JsonNode value = remote(host, "/internal/get?store=" + enc(store) + "&key=" + enc(key)
                    + "&partition=" + owner.partition()).get("value");
            return value == null || value.isNull() ? null : JsonSerde.MAPPER.convertValue(value, type);
        }

        <V> List<KeyValue<String, V>> range(String store, String prefix, Class<V> type) {
            return range(store, prefix, prefix + '\uffff', type);
        }

        <V> List<KeyValue<String, V>> range(String store, String from, String to, Class<V> type) {
            List<KeyValue<String, V>> rows = new ArrayList<>();
            Set<Integer> covered = new TreeSet<>();
            for (StreamsMetadata instance : streams.streamsMetadataForStore(store)) {
                HostInfo host = instance.hostInfo();
                servedBy.add(host.host() + ":" + host.port());
                if (host.equals(self)) {
                    LocalRows local = localRange(store, from, to);
                    local.rows().forEach(kv -> rows.add(KeyValue.pair(kv.key, type.cast(kv.value))));
                    covered.addAll(local.partitions());
                } else {
                    JsonNode answer = remote(host, "/internal/range?store=" + enc(store) + "&from=" + enc(from) + "&to=" + enc(to));
                    for (JsonNode row : answer.get("rows")) {
                        rows.add(KeyValue.pair(row.get("key").asText(), JsonSerde.MAPPER.convertValue(row.get("value"), type)));
                    }
                    answer.get("partitions").forEach(partition -> covered.add(partition.asInt()));
                }
            }
            // A partition whose task is moving belongs to no instance for a moment; refuse a partial answer.
            int partitions = partitionCount(store);
            if (covered.size() < partitions) {
                throw new Unavailable(store + ": " + covered.size() + " of " + partitions + " partitions available");
            }
            return rows;
        }

        Map<String, Object> done() {
            body.put("servedBy", servedBy);
            body.put("queryMicros", (System.nanoTime() - started) / 1_000);
            body.put("servedAt", System.currentTimeMillis());
            return body;
        }
    }

    private JsonNode remote(HostInfo host, String pathAndQuery) {
        try {
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder(URI.create("http://" + host.host() + ":" + host.port() + pathAndQuery))
                            .timeout(Duration.ofSeconds(3)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 503) throw new Unavailable(host + " is not ready: " + resp.body());
            if (resp.statusCode() != 200) throw new IllegalStateException(host + " returned " + resp.statusCode() + ": " + resp.body());
            return JsonSerde.MAPPER.readTree(resp.body());
        } catch (IOException e) {
            throw new Unavailable(host + " did not answer: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Unavailable("interrupted");
        }
    }

    /** Reads one partition of this instance's active copy of a store; throws if that task isn't ready here. */
    private Object localGet(String store, String key, int partition) {
        if (!useIqV2) {
            ReadOnlyKeyValueStore<String, Object> kv = streams.store(StoreQueryParameters
                    .fromNameAndType(store, QueryableStoreTypes.<String, Object>keyValueStore()).withPartition(partition));
            return kv.get(key);
        }
        StateQueryResult<Object> result = streams.query(StateQueryRequest.inStore(store)
                .withQuery(KeyQuery.<String, Object>withKey(key)).withPartitions(Set.of(partition)).requireActive());
        QueryResult<Object> answer = result.getPartitionResults().get(partition);
        if (answer == null || answer.isFailure()) {
            throw new Unavailable(store + " partition " + partition + (answer == null ? " not here" : ": " + answer.getFailureMessage()));
        }
        return answer.getResult();
    }

    private record LocalRows(List<KeyValue<String, Object>> rows, List<Integer> partitions) {
    }

    /** Range-reads every partition of a store that this instance owns, and says which partitions those were. */
    private LocalRows localRange(String store, String from, String to) {
        int sub = subtopologyOf.get(store);
        List<Integer> partitions = new ArrayList<>();
        streams.metadataForLocalThreads().forEach(thread -> thread.activeTasks().forEach(task -> {
            if (task.taskId().subtopology() == sub) partitions.add(task.taskId().partition());
        }));
        partitions.sort(null);
        List<KeyValue<String, Object>> rows = new ArrayList<>();
        for (int partition : partitions) {
            if (!useIqV2) {
                ReadOnlyKeyValueStore<String, Object> kv = streams.store(StoreQueryParameters
                        .fromNameAndType(store, QueryableStoreTypes.<String, Object>keyValueStore()).withPartition(partition));
                try (KeyValueIterator<String, Object> it = kv.range(from, to)) {
                    it.forEachRemaining(rows::add);
                }
                continue;
            }
            StateQueryResult<KeyValueIterator<String, Object>> result = streams.query(StateQueryRequest.inStore(store)
                    .withQuery(RangeQuery.<String, Object>withRange(from, to))
                    .withPartitions(Set.of(partition)).requireActive());
            QueryResult<KeyValueIterator<String, Object>> answer = result.getPartitionResults().get(partition);
            if (answer == null || answer.isFailure()) throw new Unavailable(store + " partition " + partition + " not ready");
            try (KeyValueIterator<String, Object> it = answer.getResult()) {
                it.forEachRemaining(rows::add);
            }
        }
        return new LocalRows(rows, partitions);
    }

    /** Partitions of a store, read once from its changelog topic. */
    private int partitionCount(String store) {
        return partitionCounts.computeIfAbsent(store, name -> {
            String changelog = clientConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-" + name + "-changelog";
            try {
                synchronized (this) {
                    if (admin == null) admin = Admin.create(clientConfig);
                }
                return admin.describeTopics(List.of(changelog)).allTopicNames().get(10, TimeUnit.SECONDS)
                        .get(changelog).partitions().size();
            } catch (Exception e) {
                throw new Unavailable("can't read partitions of " + changelog + ": " + e.getMessage());
            }
        });
    }

    private static Object nullable(Object value) {
        return value == null ? JsonSerde.MAPPER.nullNode() : value;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    /** Query parameters of an /internal request, which always names a known store. */
    private static Map<String, String> params(HttpExchange ex) {
        Map<String, String> out = query(ex);
        if (!VALUE_TYPES.containsKey(out.get("store"))) throw new NotFound("unknown store " + out.get("store"));
        return out;
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> out = new HashMap<>();
        String query = ex.getRequestURI().getRawQuery();
        if (query == null) return out;
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) out.put(pair.substring(0, eq), URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return out;
    }

    private interface Handler {
        Object handle() throws Exception;
    }

    private static final class NotFound extends RuntimeException {
        NotFound(String message) {
            super(message);
        }
    }

    /** A store or instance is moving or restarting; the caller should retry shortly. */
    private static final class Unavailable extends RuntimeException {
        Unavailable(String message) {
            super(message);
        }
    }

    private void respond(HttpExchange ex, boolean needsStores, Handler handler) throws IOException {
        int status;
        Object body;
        KafkaStreams.State state = streams.state();
        try {
            // Stores stay queryable while the group rebalances; only refuse before the first assignment or after shutdown.
            if (needsStores && state != KafkaStreams.State.RUNNING && state != KafkaStreams.State.REBALANCING) {
                throw new Unavailable("Kafka Streams is " + state);
            }
            body = handler.handle();
            status = 200;
        } catch (NotFound e) {
            status = 404;
            body = Map.of("error", e.getMessage());
        } catch (Unavailable | InvalidStateStoreException e) {
            status = 503;
            body = Map.of("error", String.valueOf(e.getMessage()), "retry", true);
        } catch (Exception e) {
            log.warn("Query failed: {}", ex.getRequestURI(), e);
            status = 500;
            body = Map.of("error", String.valueOf(e.getMessage()));
        }
        byte[] bytes = JsonSerde.MAPPER.writeValueAsString(body).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }
}
