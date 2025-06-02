---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis RPC with FastStream

**FastStream** `RedisBroker` provides the powerful capability to perform Remote Procedure Calls (RPC) using **Redis**. This feature enables you to send a message and await a response, effectively creating a synchronous request-response pattern over the inherently asynchronous **Redis** messaging system. Below is the guide to set up and utilize the **Redis RPC** publishing feature with **FastStream**.

!!! note
    The **RPC** feature is implemented over **Redis Pub/Sub** independently of the original subscriber type.

## RPC with Redis Overview

In a traditional publish/subscribe setup, the publishing party sends messages without expecting any direct response from the subscribers. However, with RPC, the publisher sends a message and waits for a response from the subscriber, which can then be used for subsequent operations or processing.

**FastStream** allows you to define RPC-style communication channels, lists, or streams by using the `RedisBroker`'s publishing function with the `rpc` flag set to `True`.

## Implementing Redis RPC in FastStream

To implement **Redis** RPC with `RedisBroker` in **FastStream**, follow the steps below:

1. Initiate your **FastStream** application with **RedisBroker**

    ```python linenums="1"
    {!> docs_src/redis/rpc/app.py [ln:4-5] !}
    ```

2. Define subscriber handlers for various **Redis** data types (e.g., channel, list, stream) that can process incoming messages and return responses.

    ```python linenums="1"
    {!> docs_src/redis/rpc/app.py [ln:8-23] !}
    ```

3. Send RPC messages through `RedisBroker` and await responses on the correct data type.

    Additionally, you can set a `timeout` to decide how long the publisher should wait for a response before timing out.

    ```python linenums="1" hl_lines="5 12 19"
    {!> docs_src/redis/rpc/app.py [ln:26-49] !}
    ```

In this example, we assert that the `msg` sent is the same as the response received from the subscriber, demonstrating an operational RPC pattern over three different **Redis** data types.

## Full Example of Redis RPC with FastStream

Combining all the code snippets above, here is the complete example of how to set up **Redis RPC** with **FastStream** `RedisBroker`:

```python linenums="1"
{! docs_src/redis/rpc/app.py !}
```

By embracing **Redis** RPC with **FastStream**, you can build sophisticated message-based architectures that require direct feedback from message processors. This feature is particularly suitable for cases where immediate processing is necessary or calling functions across different services is essential.

## Creating an RPC subscriber

To handle an RPC request, you need to create a subscriber that processes the incoming message and returns a response.
The subscriber should be decorated with `@broker.subscriber` and return either a raw value or a `Response` object.

Below is an example of a simple RPC subscriber that processes a message and returns a response.

```python hl_lines="1 8"
from faststream.redis import RedisBroker

broker = RedisBroker()

@broker.subscriber(channel="test-channel")
async def handle(msg):
    return f"Received: {msg}"
```

When the client sends a request like this:

```python hl_lines="1 8"
from faststream.redis import RedisMessage

msg: RedisMessage = await broker.request(
    "Hello, Redis!",
    channel="test-channel",
    timeout=3.0,
)
assert await msg.decode() == "Received: Hello, Redis!"
```

The subscriber processes the request and sends back the response, which is received by the client.

!!! tip
    You can use the `no_reply=True` flag in the `@broker.subscriber` decorator to suppress automatic RPC and `reply_to` responses.
    This is useful when you want the subscriber to process the message without sending a response back to the client.

## Using the Response class
The `Response` class allows you to attach metadata, such as headers, to the response message.
This is useful for adding context or tracking information to your responses.

Below is an example of how to use the `Response` class in an RPC subscriber.

```python hl_lines="1 8"
from faststream import Response
from faststream.redis import RedisBroker

broker = RedisBroker()

@broker.subscriber(channel="test-channel")
async def handle(msg):
    return Response(
        body=f"Processed: {msg}",
        headers={"x-token": "some-token"},
        correlation_id="some-correlation-id",
    )
```

When the client sends a request:

```python hl_lines="1 8"
from faststream.redis import RedisMessage

msg: RedisMessage = await broker.request(
    "Hello, Redis!",
    channel="test-channel",
    timeout=3.0,
)
assert await msg.decode() == "Processed: Hello, Redis!"
assert msg.headers == {"x-token": "some-token"}
assert msg.correlation_id == "some-correlation-id"
```

## Using the RedisResponse class
For Redis-specific use cases, you can use the `RedisResponse` class instead of the generic `Response` class.
The `RedisResponse` class extends `Response` and adds support for specifying a `maxlen` parameter,
which is useful when publishing responses to a Redis stream to limit the stream's length.

Below is an example of how to use the RedisResponse class in an RPC subscriber.

```python hl_lines="1 8"
from faststream import FastStream
from faststream.redis import RedisBroker, RedisResponse

broker = RedisBroker()
app = FastStream(broker)

@broker.subscriber(stream="test-stream")
async def handle(msg):
    return RedisResponse(
        body=f"Processed: {msg}",
        headers={"x-token": "some-token"},
        correlation_id="some-correlation-id",
        maxlen=1000,
    )
```

When the client sends a request:

```python hl_lines="1 8"
from faststream.redis import RedisMessage

msg: RedisMessage = await broker.request(
    "Hello, Redis!",
    stream="test-stream",
    timeout=3.0,
)
assert await msg.decode() == "Processed: Hello, Redis!"
assert msg.headers == {"x-token": "some-token"}
assert msg.correlation_id == "some-correlation-id"
assert msg.maxlen == 1000
```
