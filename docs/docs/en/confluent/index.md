---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Confluent Kafka Routing

## Confluent's Python Client for Apache Kafka

The Confluent Kafka Python library is developed by Confluent, the company founded by the creators of Apache Kafka. It offers a high-level Kafka producer and consumer API that integrates well with the Kafka ecosystem. The Confluent library provides a comprehensive set of features, including support for Avro serialization, schema registry integration, and various configurations to fine-tune performance.

Developed by Confluent, this library enjoys strong support from the core team behind Kafka. This often translates to better compatibility with the latest Kafka releases and a more robust feature set.

!!! note ""
    If you prefer the `aiokafka` library instead, then please refer to [aiokafka's KafkaBroker](../kafka/index.md)

## Installation

!!! warning "Available starting from v0.4.0rc0"
    Support for `Confluent` is present **starting** from the version **v0.4.0rc0**.

    Please run the following command to install it:

    ```bash
    pip install "faststream[confluent]>=0.4.0"
    ```

### FastStream Confluent KafkaBroker

The FastStream Confluent KafkaBroker is a key component of the FastStream framework that enables seamless integration with Apache Kafka using [confluent kafka](https://github.com/confluentinc/confluent-kafka-python){.external-link target="_blank"} python library. With the KafkaBroker, developers can easily connect to Kafka brokers, produce messages to Kafka topics, and consume messages from Kafka topics within their FastStream applications.

### Establishing a Connection

To connect to Kafka using the FastStream KafkaBroker module, follow these steps:

1. **Initialize the KafkaBroker instance:** Start by initializing a KafkaBroker instance with the necessary configuration, including Kafka broker address.

2. **Create your processing logic:** Write a function that will consume the incoming messages in the defined format and produce a response to the defined topic

3. **Decorate your processing function:** To connect your processing function to the desired Kafka topics you need to decorate it with `#!python @broker.subscriber(...)` and `#!python @broker.publisher(...)` decorators. Now, after you start your application, your processing function will be called whenever a new message in the subscribed topic is available and produce the function return value to the topic defined in the publisher decorator.

Here's a simplified code example demonstrating how to establish a connection to Kafka using FastStream's KafkaBroker module:

```python linenums="1"
{! docs_src/index/confluent/basic.py!}
```

This minimal example illustrates how FastStream simplifies the process of connecting to Kafka and performing basic message processing from the **in_topic** to the **out-topic**. Depending on your specific use case and requirements, you can further customize your Kafka integration with FastStream to build robust and efficient streaming applications.

For more advanced configuration options and detailed usage instructions, please refer to the FastStream Kafka documentation and the [official Kafka documentation](https://kafka.apache.org/){.external-link target="_blank"}.
