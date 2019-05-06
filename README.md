# kafka-event-monitor

Process monitor for event-driven data-flows.

Effort to increase visibility over even-driven data pipelines, by consuming events and building 
a materialized view that allow querying status by keys and metadata.

## Design

### Features

+ Consume from one or many topics to monitor events
+ Tag transactions with key value
+ Tag transactions with payload values
+ Query latest transactions
+ Query by tag
+ Query by status
+ Support Avro payloads

### Scenarios

#### Mono-topic

In this scenario an entity evolves over time and events are stored in the same topic.

The topic has one or many schemas representing the data inside.

The events stored in the topic has a value in the header or payload representing the status.

> This scenario won't be covered initially until a use-case is in place.

#### Multi-topic

In this scenario an entity evolves over time and events are stored in multiple topics.

Each topic represents an status over time (1 topic -> 1 status).

Each topic has 1 schema.

## Configuration

```yaml
- topic:
    name: topic1
    status: status1
    id:
      type: header
    metadata:
      - type: payload
        field: abc1
    schemas:
      - subject: schema1
        status: status1
        metadata:
          - type: payload
            field: abc2
```