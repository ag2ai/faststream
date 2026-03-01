---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis List Batch Subscriber

If you want to consume data in batches from a Redis list, the `#!python @broker.subscriber(...)` decorator makes it possible. By defining your consumed `msg` object as a list of messages and setting the `batch` parameter to `True` within the `ListSub` object, the subscriber will call your consuming function with a batch of messages. Let's walk through how to achieve this with the FastStream library.

## Using the Subscriber with Batching

To consume messages in batches from a Redis list, follow these steps:

### Step 1: Define Your Subscriber

In your FastStream application, define the subscriber using the `#!python @broker.subscriber(...)` decorator. Ensure that you pass a `ListSub` object with the `batch` parameter set to `True`. This configuration tells the subscriber to handle message consumption in batches from the specified Redis list.

```python linenums="1"
{! docs_src/redis/list/sub_batch.py [ln:8] !}
```

### Step 2: Implement Your Consuming Function

Create a consuming function that accepts the list of messages. The `#!python @broker.subscriber(...)` decorator will take care of collecting and grouping messages into batches.

```python linenums="1"
{! docs_src/redis/list/sub_batch.py [ln:8-10] !}
```

## Example of Consuming in Batches

Let's illustrate how to consume messages in batches from the `#!python "test-list"` Redis list with a practical example:

```python linenums="1"
{! docs_src/redis/list/sub_batch.py !}
```

In this example, the subscriber is configured to process messages in batches from the Redis list, and the consuming function is designed to handle these batches efficiently.

Consuming messages in batches is a valuable technique when you need to optimize the processing of high volumes of data in your Redis-based applications. It allows for more efficient resource utilization and can enhance the overall performance of your data processing tasks.

## Batch publishing

Also, **Redis List** is the only data structure supporting publishing in batches with **FastStream**. To send multiple messages in a single request, you just need to:

* Call `#!python broker.publish_batch("msg2", "msg2", list="test-list")`
* Or create a publisher with `#!python broker.publisher(list=ListSub("test-list", batch=True))`
