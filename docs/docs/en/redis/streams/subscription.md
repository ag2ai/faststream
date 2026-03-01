---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis Stream Basic Subscriber

To start consuming from a **Redis** stream, simply decorate your consuming function with the `#!python @broker.subscriber(...)` decorator, passing a string as the stream key.

In the following example, we will create a simple FastStream app that will consume messages from a `#!python "test-stream"` Redis stream.

The full app code looks like this:

```python linenums="1"
{! docs_src/redis/stream/sub.py !}
```

## Import FastStream and RedisBroker

To use the `#!python @broker.subscriber(...)` decorator, first, we need to import the base FastStream app and RedisBroker to create our broker.

```python linenums="1"
{! docs_src/redis/stream/sub.py [ln:1-2] !}
```

## Create a RedisBroker

Next, we will create a `RedisBroker` object and wrap it into the `FastStream` object so that we can start our app using CLI later.

```python linenums="1"
{! docs_src/redis/stream/sub.py [ln:4-5] !}
```

## Create a Function that will Consume Messages from a Redis stream

Let’s create a consumer function that will consume messages from `#!python "test-stream"` Redis stream and log them.

```python linenums="1"
{! docs_src/redis/stream/sub.py [ln:8-10] !}
```

The function decorated with the `#!python @broker.subscriber(...)` decorator will be called when a message is produced to the **Redis** stream.

The message will then be injected into the typed `msg` argument of the function, and its type will be used to parse the message.

In this example case, when the message is sent to a `#!python "test-stream"` stream, it will be received by the `handle` function, and the `logger` will log the message content.
