package no.sysco.middleware.kafka.eventmonitor;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import no.sysco.middleware.kafka.eventmonitor.http.MonitorServerBuilder;
import no.sysco.middleware.kafka.eventmonitor.core.EventTransitionAdapter;
import no.sysco.middleware.kafka.eventmonitor.storage.EventHistoryStore;
import no.sysco.middleware.kafka.eventmonitor.stream.MonitorStreamBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class EventMonitorApplication {
  final EventMonitorConfig config;

  KafkaStreams kafkaStreams;
  Server server;

  public EventMonitorApplication(EventMonitorConfig config) {
    this.config = config;
  }

  public void run() {
    final Properties streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.kafka.streams.applicationId);
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers);
    streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, config.kafka.streams.stateDir);

    final EventTransitionAdapter adapter =
        new EventTransitionAdapter(config.topicAndStatusMap(), config.topicAndIdKeyMap());

    final SchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(config.schemaRegistry.url, 10_000);
    final MonitorStreamBuilder monitorStream =
        new MonitorStreamBuilder(config.eventTopics(), adapter, schemaRegistryClient);
    kafkaStreams = new KafkaStreams(monitorStream.topology(), streamsConfig);
    kafkaStreams.start();

    final EventHistoryStore historyStore = new EventHistoryStore(kafkaStreams);
    final MonitorServerBuilder serverBuilder = new MonitorServerBuilder(historyStore, config.http.port);
    server = serverBuilder.build();
    server.start();
  }

  public void close() {
    kafkaStreams.close();
    server.close();
  }
}
