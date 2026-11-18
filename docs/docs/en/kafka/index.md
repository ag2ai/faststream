---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
title: "Async Kafka in Python with aiokafka"
description: >-
  Build async Kafka producers and consumers in Python with aiokafka and FastStream. Connect to a broker and route messages between topics with a complete code example.
search:
  boost: 10
---

# Async Kafka in Python with aiokafka

Use **aiokafka** to publish and consume Kafka messages from asynchronous Python applications. FastStream builds on this native client and adds subscriber and publisher decorators, dependency injection, message validation and in-memory testing. This guide connects to Kafka and routes messages from an input topic to an output topic.

## AIOKafka library

The `aiokafka` library is an asynchronous Kafka client for Python, built on top of the `asyncio` framework. It is designed to work seamlessly with asynchronous code, making it suitable for applications with high concurrency requirements.

!!! note ""
    If you prefer the `confluent-kafka-python` library instead, then please refer to [Confluent's KafkaBroker](../confluent/index.md)

## FastStream KafkaBroker

The FastStream KafkaBroker is a key component of the FastStream framework that enables seamless integration with Apache Kafka using [aiokafka](https://github.com/aio-libs/aiokafka) library. With the KafkaBroker, developers can easily connect to Kafka brokers, produce messages to Kafka topics, and consume messages from Kafka topics within their FastStream applications.

### Establishing a Connection

To connect to Kafka using the FastStream KafkaBroker module, follow these steps:

1. **Initialize the KafkaBroker instance:** Start by initializing a KafkaBroker instance with the necessary configuration, including Kafka broker address.

2. **Create your processing logic:** Write a function that will consume the incoming messages in the defined format and produce a response to the defined topic

3. **Decorate your processing function:** To connect your processing function to the desired Kafka topics you need to decorate it with `#!python @broker.subscriber(...)` and `#!python @broker.publisher(...)` decorators. Now, after you start your application, your processing function will be called whenever a new message in the subscribed topic is available and produce the function return value to the topic defined in the publisher decorator.

Here's a simplified code example demonstrating how to establish a connection to Kafka using FastStream's KafkaBroker module:

```python linenums="1"
{! docs_src/index/kafka/basic.py!}
```

This minimal example illustrates how FastStream simplifies the process of connecting to Kafka and performing basic message processing from the **in-topic** to the **out-topic**. Depending on your specific use case and requirements, you can further customize your Kafka integration with FastStream to build robust and efficient streaming applications.

For more advanced configuration options and detailed usage instructions, please refer to the FastStream Kafka documentation and the [official Kafka documentation](https://kafka.apache.org/){.external-link target="_blank"}.

## Consumer defaults

Both Kafka backends expose a class named `KafkaBroker`: `faststream.kafka.KafkaBroker` uses **aiokafka**, and `faststream.confluent.KafkaBroker` uses **confluent-kafka**.

| Parameter | Set on | AIOKafka default | Confluent default |
|-----------|--------|------------------|-------------------|
| `auto_offset_reset` | `@broker.subscriber(...)` | `"latest"` | `"latest"` |
| `group_id` | `@broker.subscriber(...)` | `None` | `None` (uses `"faststream-consumer-group"` internally) |
| `client_id` | `KafkaBroker(...)` | `"faststream-{version}"` | `"faststream-{version}"` |

Here, `{version}` is the installed FastStream version.

### Starting offsets

`auto_offset_reset` applies when the consumer has no committed offset or its offset is no longer available. With `"latest"`, consumption starts at the end of the partition; use `"earliest"` to start at the oldest retained message. A valid committed offset takes precedence over this setting, so `auto_offset_reset="earliest"` does not replay a group's already committed messages on restart.

### Consumer groups

With **aiokafka**, `group_id=None` disables consumer group coordination and offset commits. Without stored offsets, a restarted subscriber uses `auto_offset_reset` to choose where to start.

With **Confluent**, omitting `group_id` passes `"faststream-consumer-group"` to the underlying client. Subscribers using this default and subscribing to the same topic share a consumer group and divide its partitions among themselves. This differs from aiokafka's behavior with `group_id=None`.

Set an explicit `group_id` for subscribers that need to resume from committed offsets. Use different group IDs for applications that must each receive every message, and the same group ID for replicas that should share the work. Confluent also requires an explicit `group_id` for any acknowledgement policy other than `AckPolicy.ACK_FIRST`; see [Confluent acknowledgements](../confluent/ack.md){.internal-link}.

### Client identification

`client_id` identifies the client in Kafka logs and metrics. It does not select a consumer group or control where consumption starts. Set `group_id` and `auto_offset_reset` on the subscriber to control those behaviors.
