package no.sysco.middleware.kafka.eventmonitor.storage;

import no.sysco.middleware.kafka.eventmonitor.core.EventHistory;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import static no.sysco.middleware.kafka.eventmonitor.stream.MonitorStreamBuilder.STORE_NAME;

public class EventHistoryStore {
    final KafkaStreams kafkaStreams;

    public EventHistoryStore(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    ReadOnlyKeyValueStore<String, String> store() {
        return kafkaStreams.store(STORE_NAME, QueryableStoreTypes.keyValueStore());
    }

    public EventHistory get(String eventId) {
        String transitions = store().get(eventId);
        if (transitions == null) return null;
        else return EventHistory.create(eventId, transitions);
    }
}
