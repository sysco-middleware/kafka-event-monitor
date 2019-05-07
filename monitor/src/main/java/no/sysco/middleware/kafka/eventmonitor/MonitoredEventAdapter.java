package no.sysco.middleware.kafka.eventmonitor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.ProcessorContext;

public class MonitoredEventAdapter {
    public MonitoredEvent event(ProcessorContext context, GenericRecord genericRecord) {
        return null;
    }
}
