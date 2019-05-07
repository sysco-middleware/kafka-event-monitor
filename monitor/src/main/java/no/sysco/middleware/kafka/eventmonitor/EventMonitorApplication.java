package no.sysco.middleware.kafka.eventmonitor;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class EventMonitorApplication {
    final EventMonitorConfig config;

    KafkaStreams kafkaStreams;

    public EventMonitorApplication(EventMonitorConfig config) {
        this.config = config;
    }

    private KafkaStreams initKafkaStreams() {
        final var streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-event-monitor");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers);

        final var adapter = new MonitoredEventAdapter();

        final var schemaRegistryClient = new CachedSchemaRegistryClient(config.schemaRegistry.url, 10_000);
        final var monitorStream = new MonitorStream(config.eventTopics(), adapter, schemaRegistryClient);
        return new KafkaStreams(monitorStream.topology(), streamsConfig);
    }

    public void run() {
        kafkaStreams = initKafkaStreams();
        kafkaStreams.start();
    }

    public void close() {
        kafkaStreams.close();
    }
}
