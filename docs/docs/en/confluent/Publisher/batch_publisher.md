---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publishing in Batches

## General Overview

If you need to send your data in batches, the `#!python @broker.publisher(...)` decorator offers a convenient way to achieve this. To enable batch production, you need to perform two crucial steps:

1. When creating your publisher, set the batch argument to `True`. This configuration tells the publisher that you intend to send messages in batches.

2. In your producer function, return a tuple containing the messages you want to send as a batch. This action triggers the producer to gather the messages and transmit them as a batch to a **Kafka** broker.

Let's delve into a detailed example illustrating how to produce messages in batches to the `#!python "output_data"` topic while consuming from the `#!python "input_data_1"` topic.

## Code Example

First, let's take a look at the whole app creation and then dive deep into the steps for producing in batches. Here is the application code:

```python linenums="1"
{! docs_src/confluent/publish_batch/app.py!}
```

Below, we have highlighted key lines of code that demonstrate the steps involved in creating and using a batch publisher:

Step 1: Creation of the Publisher

```python linenums="1"
{! docs_src/confluent/publish_batch/app.py [ln:19] !}
```

Step 2: Publishing an Actual Batch of Messages

You can publish a batch by directly calling the publisher with a batch of messages you want to publish, as shown here:

```python linenums="1"
{! docs_src/confluent/publish_batch/app.py [ln:32.5,33.5,34.5] !}
```

Or you can decorate your processing function and return a batch of messages, as shown here:

```python linenums="1"
{! docs_src/confluent/publish_batch/app.py [ln:22-26] !}
```

The application in the example implements both of these ways, so feel free to use whichever option fits your needs better.

!!! note
    Also, you can publishes messages in batches right from a `broker` object: just call
    `#!python broker.publish_batch("msg2", "msg2", topic="output_data")`

## Why Publish in Batches?

In the above example, we've explored how to leverage the `#!python @broker.publisher(...)` decorator to efficiently publish messages in batches using **FastStream** and **Kafka**. By following the two key steps outlined in the previous sections, you can significantly enhance the performance and reliability of your **Kafka**-based applications.

Publishing messages in batches offers several advantages when working with **Kafka**:

1. **Improved Throughput**: Batch publishing allows you to send multiple messages in a single transmission, reducing the overhead associated with individual message delivery. This leads to improved throughput and lower latency in your **Kafka** applications.

2. **Reduced Network and Broker Load**: Sending messages in batches reduces the number of network calls and broker interactions. This optimization minimizes the load on the **Kafka** brokers and network resources, making your **Kafka** cluster more efficient.

3. **Atomicity**: Batches ensure that a group of related messages is processed together or not at all. This atomicity can be crucial in scenarios where message processing needs to maintain data consistency and integrity.

4. **Enhanced Scalability**: With batch publishing, you can efficiently scale your **Kafka** applications to handle high message volumes. By sending messages in larger chunks, you can make the most of **Kafka**'s parallelism and partitioning capabilities.
