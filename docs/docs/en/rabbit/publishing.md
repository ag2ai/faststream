---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publishing

**FastStream** `RabbitBroker` supports all regular [publishing use cases](../getting-started/publishing/index.md){.internal-link}. you can use them without any changes.

However, if you wish to further customize the publishing logic further, you should take a more deep-dive look at specific RabbitBroker parameters.

## Rabbit Publishing

`RabbitBroker` also uses the unified `publish` method (from a `publisher` object) to send messages.

However, in this case, an object of the `aio_pika.Message` class (if necessary) can be used as a message (in addition to python primitives and `pydantic.BaseModel`).

You can specify queue (used as a routing_key) and exchange (optionally) to send by their name.

```python
import asyncio
from faststream.rabbit import RabbitBroker

async def pub():
    async with RabbitBroker() as broker:
        await broker.publish(
            "Hi!",
            queue="test",
            exchange="test"
        )

asyncio.run(pub())
```

If you don't specify any exchange, the message will be send to the default one.

Also, you are able to use special **RabbitQueue** and **RabbitExchange** objects as `queue` and `exchange` arguments:

```python
from faststream.rabbit import RabbitExchange, RabbitQueue

await broker.publish(
    "Hi!",
    queue=RabbitQueue("test"),
    exchange=RabbitExchange("test")
)
```

If you specify exchange that doesn't exist, RabbitBroker will create a required one and then publish a message to it.

!!! tip
    Be accurate with it: if you have already created an **Exchange** with specific parameters and try to send a message by exchange name to it, the broker will try to create it. So, **Exchange** parameters conflict will occur.

    If you are trying to send a message to a specific **Exchange**, sending it with a defined **RabbitExchange** object is the preferred way.

## Basic Arguments

The `publish` method takes the following arguments:

* `#!python message = ""` - message to send
* `#!python exchange: str | RabbitExchange | None = None` - the exchange where the message will be sent to. If not specified - *default* is used
* `#!python queue: str | RabbitQueue = ""` - the queue where the message will be sent (since most queues use their name as the routing key, this is a human-readable version of `routing_key`)
* `#!python routing_key: str = ""` - also a message routing key, if not specified, the `queue` argument will be used

## Message Parameters

You can read more about all the available flags in the [RabbitMQ documentation](https://www.rabbitmq.com/consumers.html){.external-link target="_blank"}

* `#!python headers: dict[str, Any] | None = None` - message headers (used by consumers)
* `#!python content_type: str | None = None` - the content_type of the message being sent (set automatically, used by consumers)
* `#!python content_encoding: str | None = None` - encoding of the message (used by consumers)
* `#!python persist: bool = False` - restore messages on *RabbitMQ* reboot
* `#!python priority: int | None = None` - the priority of the message
* `#!python correlation_id: str | None = None` - message id, which helps to match the original message with the reply to it (generated automatically)
* `#!python message_id: str | None = None` - message ID (generated automatically)
* `#!python timestamp: int | float | time delta | datetime | None = None` - message sending time (set automatically)
* `#!python expiration: int | float | time delta | datetime | None = None` - message lifetime (in seconds)
* `#!python type: str | None = None` - the type of message (used by consumers)
* `#!python user_id: str | None = None` - ID of the *RabbitMQ* user who sent the message
* `#!python app_id: str | None = None` - ID of the application that sent the message (used by consumers)

## Send Flags

Arguments for sending a message:

* `#!python mandatory: bool = True` - the client is waiting for confirmation that the message will be placed in some queue (if there are no queues, return it to the sender)
* `#!python immediate: bool = False` - the client expects that there is a consumer ready to take the message to work "right now" (if there is no consumer, return it to the sender)
* `#!python timeout: int | float | None = None` - send confirmation time from *RabbitMQ*
