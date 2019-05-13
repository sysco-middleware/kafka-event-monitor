package no.sysco.middleware.kafka.eventmonitor.stream;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.Collection;
import no.sysco.middleware.kafka.eventmonitor.core.EventHistory;
import no.sysco.middleware.kafka.eventmonitor.core.EventTransition;
import no.sysco.middleware.kafka.eventmonitor.core.EventTransitionAdapter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class MonitorStreamBuilder {
  public static final String STORE_NAME = "event-history";

  final Collection<String> eventTopics;
  final EventTransitionAdapter adapter;
  final Serde<byte[]> keySerde;
  final Serde<GenericRecord> valueSerde;
  final Serde<String> stateKeySerde;
  final Serde<String> stateValueSerde;

  public MonitorStreamBuilder(Collection<String> eventTopics, EventTransitionAdapter adapter,
      SchemaRegistryClient schemaRegistryClient) {
    this.eventTopics = eventTopics;
    this.adapter = adapter;
    keySerde = Serdes.ByteArray();
    valueSerde = new GenericAvroSerde(schemaRegistryClient);
    stateKeySerde = Serdes.String();
    stateValueSerde = Serdes.String();
  }

  public Topology topology() {
    final StreamsBuilder builder = new StreamsBuilder();
    final StoreBuilder<KeyValueStore<String, String>> historyStoreBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STORE_NAME),
        stateKeySerde,
        stateValueSerde)
        .withLoggingDisabled()
        .withCachingEnabled();

    builder.addStateStore(historyStoreBuilder)
        .stream(
            eventTopics,
            Consumed.with(keySerde, valueSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        .transformValues(() -> new ValueTransformer<GenericRecord, EventTransition>() {
          ProcessorContext context;

          @Override
          public void init(ProcessorContext context) {
            this.context = context;
          }

          @Override
          public EventTransition transform(GenericRecord genericRecord) {
            return adapter.event(context, genericRecord);
          }

          @Override
          public void close() {
          }
        })
        .process(() -> new Processor<byte[], EventTransition>() {
          ProcessorContext context;
          KeyValueStore<String, String> historyStore;

          @Override
          public void init(ProcessorContext context) {
            this.context = context;
            this.historyStore = (KeyValueStore<String, String>) context.getStateStore(STORE_NAME);
          }

          @Override
          public void process(byte[] bytes, EventTransition eventTransition) {
            String transitions = historyStore.get(eventTransition.id());
            if (transitions == null) {
              EventHistory history = EventHistory.create(eventTransition);
              historyStore.put(eventTransition.id(), history.serialize());
            } else {
              EventHistory history = EventHistory.create(eventTransition.id(), transitions);
              history.addTransition(eventTransition);
              historyStore.put(eventTransition.id(), history.serialize());
            }
          }

          @Override
          public void close() {
          }
        }, STORE_NAME);

    return builder.build();
  }
}
