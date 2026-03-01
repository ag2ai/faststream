---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# AIOKafka Routing

## AIOKafka library

The `aiokafka` library, is an asynchronous Kafka client for Python, built on top of the `asyncio` framework. It is designed to work seamlessly with asynchronous code, making it suitable for applications with high concurrency requirements.

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

This minimal example illustrates how FastStream simplifies the process of connecting to Kafka and performing basic message processing from the **in_topic** to the **out-topic**. Depending on your specific use case and requirements, you can further customize your Kafka integration with FastStream to build robust and efficient streaming applications.

For more advanced configuration options and detailed usage instructions, please refer to the FastStream Kafka documentation and the [official Kafka documentation](https://kafka.apache.org/){.external-link target="_blank"}.
