package no.sysco.middleware.kafka.eventmonitor.core;

public class EventTransition {
    final String id;
    final String name;
    final long timestamp;

    public EventTransition(String id, String name, long timestamp) {
        this.id = id;
        this.name = name;
        this.timestamp = timestamp;
    }

    public String id() {
        return id;
    }
}
