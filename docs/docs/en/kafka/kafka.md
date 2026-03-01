---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Kafka Routing

## Kafka Overview

### What is Kafka?

[Kafka](https://kafka.apache.org/){.external-link target="_blank"} is an open-source distributed streaming platform developed by the Apache Software Foundation. It is designed to handle high-throughput, fault-tolerant, real-time data streaming. Kafka is widely used for building real-time data pipelines and streaming applications.

### Key Kafka Concepts

#### 1. Publish-Subscribe Model

Kafka is built around the publish-subscribe messaging model. In this model, data is published to topics, and multiple consumers can subscribe to these topics to receive the data. This decouples the producers of data from the consumers, allowing for flexibility and scalability.

#### 2. Topics

A **topic** in Kafka is a logical channel or category to which messages are published by producers and from which messages are consumed by consumers. Topics are used to organize and categorize data streams. Each topic can have multiple **partitions**, which enable Kafka to distribute data and provide parallelism for both producers and consumers.

## Kafka Topics

### Understanding Kafka Topics

Topics are fundamental to Kafka and serve as the central point of data distribution. Here are some key points about topics:

- Topics allow you to logically group and categorize messages.
- Each message sent to Kafka is associated with a specific topic.
- Topics can have one or more partitions to enable parallel processing and scaling.
- Consumers subscribe to topics to receive messages.

## Library support

`FastStream` provides two different `KafkaBroker`s based on the following libraries:

- [Confluent Kafka](https://github.com/confluentinc/confluent-kafka-python){.external-link target="_blank"}
- [aiokafka](https://github.com/aio-libs/aiokafka){.external-link target="_blank"}

### Confluent's Python Client for Apache Kafka

The Confluent Kafka Python library is developed by Confluent, the company founded by the creators of Apache Kafka. It offers a high-level Kafka producer and consumer API that integrates well with the Kafka ecosystem. The Confluent library provides a comprehensive set of features, including support for Avro serialization, schema registry integration, and various configurations to fine-tune performance.

Developed by Confluent, this library enjoys strong support from the core team behind Kafka. This often translates to better compatibility with the latest Kafka releases and a more robust feature set.

Check out [Confluent's KafkaBroker](../confluent/index.md).

### AIOKafka library

The `aiokafka` library, is an asynchronous Kafka client for Python, built on top of the `asyncio` framework. It is designed to work seamlessly with asynchronous code, making it suitable for applications with high concurrency requirements.

Check out [aiokafka's KafkaBroker](../kafka/index.md).
