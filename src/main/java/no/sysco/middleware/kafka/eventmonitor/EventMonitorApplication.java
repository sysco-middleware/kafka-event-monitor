package no.sysco.middleware.kafka.eventmonitor;

import no.sysco.middleware.kafka.eventmonitor.stream.MonitorStream;
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

        final var monitorStream = new MonitorStream(config.eventTopics());
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
