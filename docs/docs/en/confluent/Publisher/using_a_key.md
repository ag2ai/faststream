---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Using a Partition Key

Partition keys are a crucial concept in Apache **Kafka**, enabling you to determine the appropriate partition for a message. This ensures that related messages are kept together in the same partition, which can be invaluable for maintaining order or grouping related messages for efficient processing. Additionally, **Kafka** utilizes partitioning to distribute load across multiple brokers and scale horizontally, while replicating data across brokers provides fault tolerance.

You can specify your partition keys when utilizing the `#!python @KafkaBroker.publisher(...)` decorator in **FastStream**. This guide will walk you through the process of using partition keys effectively.

## Publishing with a Partition Key

To publish a message to a **Kafka** topic using a partition key, follow these steps:

### Step 1: Define the Publisher

In your FastStream application, define the publisher using the `#!python @KafkaBroker.publisher(...)` decorator. This decorator allows you to configure various aspects of message publishing, including the partition key.

```python linenums="1"
{! docs_src/confluent/publish_with_partition_key/app.py [ln:17] !}
```

### Step 2: Pass the Key

When you're ready to publish a message with a specific key, simply include the `key` parameter in the `publish` function call. This key parameter is used to determine the appropriate partition for the message.

```python linenums="1"
{! docs_src/confluent/publish_with_partition_key/app.py [ln:25.5] !}
```

## Example Application

Let's examine a complete application example that consumes messages from the `#!python "input_data"` topic and publishes them with a specified key to the `#!python "output_data"` topic. This example will illustrate how to incorporate partition keys into your **Kafka**-based applications:

```python linenums="1"
{! docs_src/confluent/publish_with_partition_key/app.py [ln:1-25] !}
```

As you can see, the primary difference from standard publishing is the inclusion of the `key` parameter in the `publish` call. This key parameter is essential for controlling how **Kafka** partitions and processes your messages.

In summary, using partition keys in **Apache Kafka** is a fundamental practice for optimizing message distribution, maintaining order, and achieving efficient processing. It is a key technique for ensuring that your **Kafka**-based applications scale gracefully and handle large volumes of data effectively.
