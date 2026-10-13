package io.confluent.demo.riverair;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * River Air operations view on Kafka Streams.
 *
 * <pre>
 *   INSTANCE_ID=1 build/install/river-air-ops-kstreams/bin/river-air-ops-kstreams config/local.properties
 * </pre>
 *
 * Several instances share the work under one application.id. INSTANCE_ID (default 1) gives each its own
 * state directory and HTTP port (7070 + id - 1), which is also its application.server for routing queries.
 * NUM_STREAM_THREADS sets threads per instance (default 2). For Confluent Cloud, the credentials come from
 * the environment: BOOTSTRAP_SERVERS, CLUSTER_API_KEY/SECRET, SCHEMA_REGISTRY_URL, and
 * SCHEMA_REGISTRY_API_KEY/SECRET (see config/cloud.properties).
 */
public final class OpsApp {
    private static final Logger log = LoggerFactory.getLogger(OpsApp.class);

    private OpsApp() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: OpsApp <config.properties>");
            System.exit(2);
        }
        Properties props = load(args[0]);
        int instance = Integer.parseInt(System.getenv().getOrDefault("INSTANCE_ID", "1"));
        int port = Integer.parseInt(props.getProperty("http.base.port", "7070")) + instance - 1;
        HostInfo self = new HostInfo("localhost", port);
        boolean useIqV2 = "v2".equals(props.getProperty("query.api", "v1"));
        props.remove("http.base.port");
        props.remove("query.api");
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, self.host() + ":" + self.port());
        props.put(StreamsConfig.STATE_DIR_CONFIG, props.getProperty(StreamsConfig.STATE_DIR_CONFIG) + "/instance-" + instance);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, System.getenv().getOrDefault("NUM_STREAM_THREADS", "2"));
        log.info("Instance {}: {} stream threads, queries on :{}", instance, props.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG), port);

        Map<String, Object> serdeConfig = new HashMap<>();
        props.forEach((k, v) -> serdeConfig.put((String) k, v));
        Topology topology = OpsTopology.build(serdeConfig);
        log.info("Topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        MaxFailuresHandler onThreadFailure = new MaxFailuresHandler(5, Duration.ofMinutes(1));
        streams.setUncaughtExceptionHandler(e -> {
            log.error("Stream thread failed", e);
            return onThreadFailure.handle(e);
        });
        QueryServer queries = new QueryServer(streams, self, useIqV2, topology.describe(), props);

        CountDownLatch done = new CountDownLatch(1);
        streams.setStateListener((now, before) -> {
            log.info("Kafka Streams {} -> {}", before, now);
            if (now == KafkaStreams.State.ERROR || now == KafkaStreams.State.NOT_RUNNING) done.countDown();
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            queries.stop();
            streams.close(Duration.ofSeconds(30)); // bounded, so a stuck stream thread can't keep the process alive
            done.countDown();
        }, "shutdown"));

        streams.start();
        queries.start();
        done.await();
    }

    private static Properties load(String path) throws IOException {
        Properties props = new Properties();
        try (Reader in = new FileReader(path)) {
            props.load(in);
        }
        // ${VAR} placeholders are filled from the environment, so credentials stay out of the file.
        for (String name : props.stringPropertyNames()) {
            String value = props.getProperty(name);
            for (Map.Entry<String, String> env : System.getenv().entrySet()) {
                value = value.replace("${" + env.getKey() + "}", env.getValue());
            }
            if (value.contains("${")) throw new IllegalArgumentException(name + " needs an environment variable: " + value);
            props.setProperty(name, value);
        }
        String applicationId = System.getenv("APPLICATION_ID"); // a second, independent copy of the app
        if (applicationId != null) props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "river-air-ops");
        props.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        String stateDir = System.getenv("STATE_DIR"); // one per cluster, so state never crosses clusters
        if (stateDir != null) props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.putIfAbsent(StreamsConfig.STATE_DIR_CONFIG, "build/state");
        props.putIfAbsent("auto.offset.reset", "earliest");
        props.putAll(baseline());
        return props;
    }

    /** Settings every environment shares: KIP-1071, explicit names, error handling, metrics. */
    static Properties baseline() {
        Properties props = new Properties();
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, "streams");
        props.put(StreamsConfig.ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG, true);
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueProcessingExceptionHandler.class);
        props.put(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 300_000);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "lz4");
        return props;
    }
}
