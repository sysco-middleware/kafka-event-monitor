package no.sysco.middleware.kafka.eventmonitor.core;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Monitored event, holding status transitions.
 * <p>
 * This event will not include metadata indexing as it will increase cost of storage
 * and increase complexity. This could be delegated to source system, and id should be well defined.
 * <p>
 * Idea: Potentially we could include multiple ids in order to map different systems identifiers.
 */
public class EventHistory {
    final String id;
    final List<Transition> transitions;

    EventHistory(String id, List<Transition> transitions) {
        this.id = id;
        this.transitions = transitions;
    }

    public static EventHistory create(EventTransition eventTransition) {
        return new EventHistory(
                eventTransition.id,
                List.of(new Transition(eventTransition.name, eventTransition.timestamp)));
    }

    public static EventHistory create(String id, String transitions) {
        return new EventHistory(id, deserialize(transitions));
    }

    public static List<Transition> deserialize(String serialized) {
        return Stream.of(serialized.split("\\|"))
                .map(s -> s.split(":"))
                .map(p -> new Transition(p[0], Long.valueOf(p[1])))
                .collect(Collectors.toList());
    }

    public String serialize() {
        return transitions.stream()
                .map(t -> t.name.concat(":").concat(String.valueOf(t.timestamp)))
                .collect(Collectors.joining("|"));
    }

    public void addTransition(EventTransition eventTransition) {
        this.transitions.add(new Transition(eventTransition.name, eventTransition.timestamp));
    }

    static class Transition {
        final String name;
        final long timestamp;

        Transition(String name, long timestamp) {
            this.name = name;
            this.timestamp = timestamp;
        }
    }
}
