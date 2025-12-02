---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Basic Subscriber

To start consuming from a **Kafka** topic, simply decorate your consuming function with a `#!python @broker.subscriber(...)` decorator, passing a string as a topic key.

In the following example, we will create a simple FastStream app that will consume `HelloWorld` messages from a `#!python "hello_world"` topic.

The full app code looks like this:

```python linenums="1"
{! docs_src/kafka/consumes_basics/app.py!}
```

## Import FastStream and KafkaBroker

To use the `#!python @broker.subscriber(...)` decorator, first, we need to import the base FastStream app KafkaBroker to create our broker.

```python linenums="1"
{! docs_src/kafka/consumes_basics/app.py [ln:3-4] !}
```

## Define the HelloWorld Message Structure

Next, you need to define the structure of the messages you want to consume from the topic using Pydantic. For the guide, we’ll stick to something basic, but you are free to define any complex message structure you wish in your project.

```python linenums="1"
{! docs_src/kafka/consumes_basics/app.py [ln:7-12] !}
```

## Create a KafkaBroker

Next, we will create a `KafkaBroker` object and wrap it into the `FastStream` object so that we can start our app using CLI later.

```python linenums="1"
{! docs_src/kafka/consumes_basics/app.py [ln:15-16] !}
```

## Create a Function that will Consume Messages from a Kafka hello-world Topic

Let’s create a consumer function that will consume `HelloWorld` messages from `#!python "hello_world"` topic and log them.

```python linenums="1"
{! docs_src/kafka/consumes_basics/app.py [ln:19-21] !}
```

The function decorated with the `#!python @broker.subscriber(...)` decorator will be called when a message is produced to **Kafka**.

The message will then be injected into the typed `msg` argument of the function, and its type will be used to parse the message.

In this example case, when the message is sent to a `#!python "hello_world"` topic, it will be parsed into a `HelloWorld` class, and the `on_hello_world` function will be called with the parsed class as the `msg` argument value.

### Pattern data access

You can also use pattern subscription feature to encode some data directly in the topic name. With **FastStream** you can easily access this data using the following code:

```python hl_lines="3 6"
from faststream import Path

@broker.subscriber(pattern="logs.{level}")
async def base_handler(
    body: str,
    level: str = Path(),
):
    ...
```

## Concurrent processing

There are two possible modes of concurrent message processing:

* With `AckPolicy.ACK_FIRST` and `max_workers` > 1, a handler processes all messages concurrently in a at-most-once semantic.
* With `AckPolicy.NACK_ON_ERROR` and `max_workers` > 1, processing is concurrent between topic partitions and sequential within a partition to ensure reliable at-least-once processing. Maximum concurrency is achieved when total number of workers across all application instances running workers in the same consumer group is equal to the number of partitions in the topic. Increasing worker count beyond that will result in idle workers as not more than one consumer from a consumer group can be consuming from the same partition.
