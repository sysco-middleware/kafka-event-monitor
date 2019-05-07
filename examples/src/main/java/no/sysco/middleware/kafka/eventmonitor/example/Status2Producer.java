package no.sysco.middleware.kafka.eventmonitor.example;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.util.Properties;

public class Status2Producer {
    public static void main(String[] args) throws Exception {
        var producerConfigs = new Properties();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
        producerConfigs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        var producer = new KafkaProducer<String, GenericRecord>(producerConfigs);

        var schema = new Schema.Parser().parse(new File("topic-value.avsc"));

        var value = new GenericData.Record(schema);
        value.put("id", "125");
        value.put("value", 3);

        var record = new ProducerRecord<String, GenericRecord>("topic2", "key", value);
        var metadata = producer.send(record).get();
        System.out.println(metadata);
    }
}
