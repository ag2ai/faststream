---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# RPC over NATS

Because **NATS** has zero cost for creating new subjects, we can easily set up a new subject consumer just for the one response message. This way, your request message will be published to one topic, and the response message will be consumed from another one (temporary subject), which allows you to use regular **FastStream RPC** syntax in the **NATS** case too.

!!! tip
    **FastStream RPC** over **NATS** works in both the *NATS-Core* and *NATS-JS* cases as well, but in the *NATS-JS* case, you have to specify the expected `stream` as a publish argument.

## Blocking Request

**FastStream** provides you with the ability to send a blocking RPC request over *NATS* in a very simple way.

Just send a message like a regular one and get a response synchronously.

It is very close to the common **requests** syntax:

```python linenums="1" hl_lines="3"
from faststream.nats import NatsMessage

msg: NatsMessage = await broker.request("Hello, NATS!", subject="test")
```

## Reply-To

Also, if you want to create a permanent request-reply data flow, probably, you should create a permanent subject to consume responses.

So, if you have such one, you can specify it with the `reply_to` argument. This way, **FastStream** will send a response to this subject automatically.

```python linenums="1" hl_lines="1 8"
@broker.subscriber("response-subject")
async def consume_responses(msg):
    ...

await broker.publish(
    "Hi!",
    subject="test",
    reply_to="response-subject",
)
```

## Creating an RPC subscriber

To handle an RPC request, you need to create a subscriber that processes the incoming message and returns a response.
The subscriber should be decorated with `#!python @broker.subscriber` and return either a raw value or a `Response` object.

Below is an example of a simple RPC subscriber that processes a message and returns a response.

```python linenums="1" hl_lines="7"
from faststream.nats import NatsBroker

broker = NatsBroker()

@broker.subscriber("test")
async def handle(msg):
    return f"Received: {msg}"
```

When the client sends a request like this:

```python linenums="1" hl_lines="3"
from faststream.nats import NatsMessage

msg: NatsMessage = await broker.request("Hello, NATS!", subject="test")
assert msg.body == b"Received: Hello, NATS!"
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
from faststream.nats import NatsBroker

broker = NatsBroker()

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
from faststream.nats import NatsMessage

msg: NatsMessage = await broker.request("Hello, NATS!", subject="test")
assert msg.body == b"Processed: Hello, NATS!"
assert msg.headers == {"x-token": "some-token"}
assert msg.correlation_id == "some-correlation-id"
```

## Using the NatsResponse class

For NATS-specific use cases, you can use the `NatsResponse` class instead of the generic `Response` class.

The `NatsResponse` class extends `Response` and adds support for specifying a `stream` parameter. This ensures the response is published to the correct stream in a JetStream context.

```python linenums="1" hl_lines="1 7-13"
from faststream.nats import NatsBroker, NatsResponse

broker = NatsBroker()

@broker.subscriber("test", stream="stream")
async def handle(msg):
    return NatsResponse(
        body=f"Processed: {msg}",
        headers={"x-token": "some-token"},
        correlation_id="some-correlation-id",
        stream="stream",
    )
```
