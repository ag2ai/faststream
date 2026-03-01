---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# RPC over RMQ

## Blocking Request

**FastStream** provides you with the ability to send a blocking RPC request over *RabbitMQ* in a very simple way.

It uses the [**Direct Reply-To**](https://www.rabbitmq.com/direct-reply-to.html){.external-link target="_blank"} *RabbitMQ* feature, so you don't need to create any queues to consume a response.

Just send a message like a regular one and get a response synchronously.

It is very close to common **requests** syntax:

```python linenums="1" hl_lines="3"
from faststream.rabbit import RabbitMessage

msg: RabbitMessage = await broker.request("Hello, RabbitMQ!", queue="test")
```

## Reply-To

Also, if you want to create a permanent request-reply data flow, probably, you should create a permanent queue to consume responses.

So, if you have such one, you can specify it with the `reply_to` argument. This way, **FastStream** will send a response to this queue automatically.

```python linenums="1" hl_lines="1 8"
@broker.subscriber("response-queue")
async def consume_responses(msg):
    ...

await broker.publish(
    "Hello, RabbitMQ!",
    queue="test",
    reply_to="response-queue",
)
```

## Creating an RPC subscriber

To handle an RPC request, you need to create a subscriber that processes the incoming message and returns a response.
The subscriber should be decorated with `#!python @broker.subscriber` and return either a raw value or a `Response` object.

Below is an example of a simple RPC subscriber that processes a message and returns a response.

```python linenums="1" hl_lines="7"
from faststream.rabbit import RabbitBroker

broker = RabbitBroker()

@broker.subscriber("test")
async def handle(msg):
    return f"Received: {msg}"
```

When the client sends a request like this:

```python linenums="1" hl_lines="3"
from faststream.rabbit import RabbitMessage

msg: RabbitMessage = await broker.request("Hello, RabbitMQ!", queue="test")
assert msg.body == b"Received: Hello, RabbitMQ!"
```

The subscriber processes the request and sends back the response, which is received by the client.

!!! tip
    You can use the `no_reply=True` flag in the `#!python @broker.subscriber` decorator to suppress automatic RPC and `reply_to` responses.
    This is useful when you want the subscriber to process the message without sending a response back to the client.

## Using the Response class

The `Response` class allows you to attach metadata, such as headers, to the response message.
This is useful for adding context or tracking information to your responses.

Below is an example of how to use the `Response` class in an RPC subscriber.

```python linenums="1" hl_lines="1 8-12"
from faststream import Response
from faststream.rabbit import RabbitBroker

broker = RabbitBroker()

@broker.subscriber("test")
async def handle(msg):
    return Response(
        body=f"Processed: {msg}",
        headers={"x-token": "some-token"},
        correlation_id="some-correlation-id",
    )
```

When the client sends a request:

```python linenums="1" hl_lines="4-6"
from faststream.rabbit import RabbitMessage

msg: RabbitMessage = await broker.request("Hello, RabbitMQ!", queue="test")
assert msg.body == b"Processed: Hello, RabbitMQ!"
assert msg.headers == {"x-token": "some-token"}
assert msg.correlation_id == "some-correlation-id"
```

## Using the RabbitResponse class

For RabbitMQ-specific use cases, you can use the `RabbitResponse` class instead of the generic `Response` class.

The `RabbitResponse` class extends `Response` and adds support for RabbitMQ-specific message properties, such as `message_id`, `priority`, `expiration`, and more.

This is particularly useful when you need fine-grained control over the message properties in a RabbitMQ context.

Below is an example of how to use the `RabbitResponse` class in an RPC subscriber.

```python linenums="1" hl_lines="1 7-14"
from faststream.rabbit import RabbitBroker, RabbitResponse

broker = RabbitBroker()

@broker.subscriber("test")
async def handle(msg):
    return RabbitResponse(
        body=f"Processed: {msg}",
        headers={"x-token": "some-token"},
        correlation_id="some-correlation-id",
        message_id="unique-message-id",
        priority=1,
        mandatory=True,
    )
```
