package no.sysco.middleware.kafka.eventmonitor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.*;
import java.util.stream.Collectors;

class EventMonitorConfig {

    final KafkaConfig kafka;
    final List<EventTopicConfig> topics;
    final SchemaRegistryConfig schemaRegistry;
    final HttpConfig http;

    EventMonitorConfig(KafkaConfig kafka, List<EventTopicConfig> topics, SchemaRegistryConfig schemaRegistry, HttpConfig http) {
        this.kafka = kafka;
        this.topics = topics;
        this.schemaRegistry = schemaRegistry;
        this.http = http;
    }

    static EventMonitorConfig load() {
        final var config = ConfigFactory.load();
        final var kafka = KafkaConfig.load(config.getConfig("event-monitor.kafka"));
        final var topics = config.getConfigList("event-monitor.topics").stream()
                .map(EventTopicConfig::load)
                .collect(Collectors.toList());
        final var schemaRegistry = SchemaRegistryConfig.load(config.getConfig("event-monitor.schema-registry"));
        final var http = HttpConfig.load(config.getConfig("event-monitor.http"));
        return new EventMonitorConfig(kafka, topics, schemaRegistry, http);
    }

    public Collection<String> eventTopics() {
        return topics.stream().map(EventTopicConfig::name).collect(Collectors.toList());
    }

    public Map<String, String> topicAndStatusMap() {
        return topics.stream().collect(Collectors.toMap(topic -> topic.name, topic -> topic.status));
    }

    public Map<String, String> topicAndIdKeyMap() {
        return topics.stream().collect(Collectors.toMap(topic -> topic.name, topic -> topic.id.key));
    }

    static class KafkaConfig {
        final String bootstrapServers;
        final StreamsConfig streams;

        KafkaConfig(String bootstrapServers, StreamsConfig streams) {
            this.bootstrapServers = bootstrapServers;
            this.streams = streams;
        }

        static KafkaConfig load(Config kafka) {
            var streams = StreamsConfig.load(kafka.getConfig("streams"));
            return new KafkaConfig(kafka.getString("bootstrap-servers"), streams);
        }

        static class StreamsConfig {
            final String applicationId;
            final String stateDir;

            StreamsConfig(String applicationId, String stateDir) {
                this.applicationId = applicationId;
                this.stateDir = stateDir;
            }

            static StreamsConfig load(Config config) {
                return new StreamsConfig(config.getString("application-id"), config.getString("state-dir"));
            }
        }
    }

    static class SchemaRegistryConfig {
        final String url;

        SchemaRegistryConfig(String url) {
            this.url = url;
        }

        static SchemaRegistryConfig load(Config config) {
            return new SchemaRegistryConfig(config.getString("url"));
        }
    }

    static class EventTopicConfig extends EventHolderConfig {
        EventTopicConfig(String name, String status, ParseConfig id, Set<ParseConfig> metadata) {
            super(name, status, id, metadata);
        }

        static EventTopicConfig load(Config config) {
            final var name = config.getString("name");
            final var status = config.getString("status");
            final var id = ParseConfig.load(config.getConfig("id"));
            final var metadata =
                    config.hasPath("metadata") ?
                            config.getConfigList("metadata").stream()
                                    .map(ParseConfig::load)
                                    .collect(Collectors.toSet()) :
                            new HashSet<ParseConfig>();
            return new EventTopicConfig(name, status, id, metadata);
        }
    }

//    static class SchemaConfig extends EventHolderConfig {
//        SchemaConfig(String name, String status, ParseConfig id, Set<ParseConfig> metadata) {
//            super(name, status, id, metadata);
//        }
//    }

    static class EventHolderConfig {
        final String name;
        final String status;
        final ParseConfig id;
        final Set<ParseConfig> metadata;

        EventHolderConfig(String name, String status, ParseConfig id, Set<ParseConfig> metadata) {
            this.name = name;
            this.status = status;
            this.id = id;
            this.metadata = metadata;
        }

        String name() {
            return name;
        }
    }

    static class ParseConfig {
//        final Source source;
        final String key;

        ParseConfig(String key) {
//            this.source = source;
            this.key = key;
        }

        static ParseConfig load(Config config) {
//            final var source = config.getEnum(Source.class, "source");
            final var key = config.getString("key");
            return new ParseConfig(key);
        }

//        public enum Source {
//            VALUE, HEADER, KEY
//        }
    }

    static class HttpConfig {
        final int port;

        HttpConfig(int port) {
            this.port = port;
        }

        static HttpConfig load(Config config) {
            return new HttpConfig(config.getInt("port"));
        }
    }
}
