package no.sysco.middleware.kafka.eventmonitor.core;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

public class EventTransitionAdapter {

    final Map<String, String> topicAndStatusMap;
    final Map<String, String> topicAndIdKeyMap;

    public EventTransitionAdapter(Map<String, String> topicAndStatusMap, Map<String, String> topicAndIdKeyMap) {
        this.topicAndStatusMap = topicAndStatusMap;
        this.topicAndIdKeyMap = topicAndIdKeyMap;
    }

    public EventTransition event(ProcessorContext context, GenericRecord genericRecord) {
        final var idKey = topicAndIdKeyMap.get(context.topic());
        final var idUtf8 = (Utf8) genericRecord.get(idKey);
        final var id = new String(idUtf8.getBytes());
        return new EventTransition(id, topicAndStatusMap.get(context.topic()), context.timestamp());
    }
}
