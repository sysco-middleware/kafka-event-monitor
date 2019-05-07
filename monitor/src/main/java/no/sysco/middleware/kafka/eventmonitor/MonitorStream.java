package no.sysco.middleware.kafka.eventmonitor;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;

public class MonitorStream {
    final Collection<String> eventTopics;
    final MonitoredEventAdapter adapter;
    final Serde<byte[]> keySerde;
    final Serde<GenericRecord> valueSerde;

    public MonitorStream(Collection<String> eventTopics, MonitoredEventAdapter adapter, SchemaRegistryClient schemaRegistryClient) {
        this.eventTopics = eventTopics;
        this.adapter = adapter;
        keySerde = Serdes.ByteArray();
        valueSerde = new GenericAvroSerde(schemaRegistryClient);
    }

    public Topology topology() {
        final var builder = new StreamsBuilder();
        builder.stream(
                eventTopics,
                Consumed.with(keySerde, valueSerde)
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .transformValues(() -> new ValueTransformer<GenericRecord, MonitoredEvent>() {
                    ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public MonitoredEvent transform(GenericRecord genericRecord) {
                        return adapter.event(context, genericRecord);
                    }

                    @Override
                    public void close() {
                    }
                })
                .foreach((key, monitoredEvent) -> System.out.println(monitoredEvent));
        return builder.build();
    }
}
