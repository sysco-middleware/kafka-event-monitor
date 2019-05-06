package no.sysco.middleware.kafka.eventmonitor.stream;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Collection;

public class MonitorStream {
    final Collection<String> eventTopics;

    public MonitorStream(Collection<String> eventTopics) {
        this.eventTopics = eventTopics;
    }

    public Topology topology() {
        final var builder = new StreamsBuilder();
        return builder.build();
    }
}
