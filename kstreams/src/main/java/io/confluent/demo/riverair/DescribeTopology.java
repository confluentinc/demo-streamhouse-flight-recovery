package io.confluent.demo.riverair;

import org.apache.kafka.streams.TopologyDescription;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Writes the app's topology offline, for the Kafka Streams UI or a topology visualizer: the text tree
 * from {@link TopologyDescription#toString()} and the same description as JSON. It builds the same
 * topology the app runs, against a mock Schema Registry, so it never contacts Kafka.
 *
 * <pre>
 *   ./gradlew describeTopology    # writes topology.txt and topology.json in kstreams/
 * </pre>
 */
public final class DescribeTopology {

    private DescribeTopology() {
    }

    public static void main(String[] args) throws Exception {
        TopologyDescription description = OpsTopology.build(Map.of("schema.registry.url", "mock://describe")).describe();
        String prefix = args.length > 0 ? args[0] : "topology";
        Files.writeString(Path.of(prefix + ".txt"), description.toString());
        Files.writeString(Path.of(prefix + ".json"), JsonSerde.MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(toModel(description)) + System.lineSeparator());
        System.out.println("Wrote " + Path.of(prefix + ".txt").toAbsolutePath() + " and " + prefix + ".json: "
                + description.subtopologies().size() + " sub-topologies");
    }

    private static Map<String, Object> toModel(TopologyDescription description) {
        List<Map<String, Object>> subtopologies = new ArrayList<>();
        description.subtopologies().stream()
                .sorted(Comparator.comparingInt(TopologyDescription.Subtopology::id))
                .forEach(sub -> {
                    Map<String, Object> model = new LinkedHashMap<>();
                    model.put("id", sub.id());
                    List<TopologyDescription.Node> nodes = new ArrayList<>(sub.nodes());
                    nodes.sort(Comparator.comparing(TopologyDescription.Node::name));
                    model.put("nodes", nodes.stream().map(DescribeTopology::node).toList());
                    subtopologies.add(model);
                });
        List<Map<String, Object>> globalStores = new ArrayList<>();
        description.globalStores().stream()
                .sorted(Comparator.comparingInt(TopologyDescription.GlobalStore::id))
                .forEach(store -> {
                    Map<String, Object> model = new LinkedHashMap<>();
                    model.put("id", store.id());
                    model.put("source", node(store.source()));
                    model.put("processor", node(store.processor()));
                    globalStores.add(model);
                });
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("subtopologies", subtopologies);
        root.put("globalStores", globalStores);
        return root;
    }

    private static Map<String, Object> node(TopologyDescription.Node node) {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("name", node.name());
        if (node instanceof TopologyDescription.Source source) {
            model.put("type", "source");
            model.put("topics", new ArrayList<>(new TreeSet<>(source.topicSet())));
            model.put("topicPattern", source.topicPattern() == null ? null : source.topicPattern().pattern());
        } else if (node instanceof TopologyDescription.Sink sink) {
            model.put("type", "sink");
            model.put("topic", sink.topic());
            model.put("topicNameExtractor", sink.topicNameExtractor() == null ? null : sink.topicNameExtractor().toString());
        } else if (node instanceof TopologyDescription.Processor processor) {
            model.put("type", "processor");
            model.put("stores", new ArrayList<>(new TreeSet<>(processor.stores())));
        } else {
            model.put("type", "node");
        }
        model.put("predecessors", names(node.predecessors()));
        model.put("successors", names(node.successors()));
        return model;
    }

    private static List<String> names(Iterable<TopologyDescription.Node> nodes) {
        TreeSet<String> names = new TreeSet<>();
        nodes.forEach(n -> names.add(n.name()));
        return new ArrayList<>(names);
    }
}
