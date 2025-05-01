---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Custom Decoder

At this stage, the body of a **StreamMessage** is transformed into the format that it will take when it enters your handler function. This stage is the one you will need to redefine more often.

## Signature

The original decoder function has a relatively simple signature (this is a simplified version):

=== "AIOKafka"
    ```python
    from faststream.types import DecodedMessage
    from faststream.kafka import KafkaMessage

    def decoder(msg: KafkaMessage) -> DecodedMessage:
        ...
    ```

=== "Confluent"
    ```python
    from faststream.types import DecodedMessage
    from faststream.confluent import KafkaMessage

    def decoder(msg: KafkaMessage) -> DecodedMessage:
        ...
    ```

=== "RabbitMQ"
    ```python
    from faststream.types import DecodedMessage
    from faststream.rabbit import RabbitMessage

    def decoder(msg: RabbitMessage) -> DecodedMessage:
        ...
    ```

=== "NATS"
    ```python
    from faststream.types import DecodedMessage
    from faststream.nats import NatsMessage

    def decoder(msg: NatsMessage) -> DecodedMessage:
        ...
    ```

=== "Redis"
    ```python
    from faststream.types import DecodedMessage
    from faststream.redis import RedisMessage

    def decoder(msg: RedisMessage) -> DecodedMessage:
        ...
    ```


Alternatively, you can reuse the original decoder function with the following signature:

=== "AIOKafka"
    ```python
    from types import Callable, Awaitable
    from faststream.types import DecodedMessage
    from faststream.kafka import KafkaMessage

    async def decoder(
        msg: KafkaMessage,
        original_decoder: Callable[[KafkaMessage], Awaitable[DecodedMessage]],
    ) -> DecodedMessage:
        return await original_decoder(msg)
    ```

=== "Confluent"
    ```python
    from types import Callable, Awaitable
    from faststream.types import DecodedMessage
    from faststream.confluent import KafkaMessage

    async def decoder(
        msg: KafkaMessage,
        original_decoder: Callable[[KafkaMessage], Awaitable[DecodedMessage]],
    ) -> DecodedMessage:
        return await original_decoder(msg)
    ```

=== "RabbitMQ"
    ```python
    from types import Callable, Awaitable
    from faststream.types import DecodedMessage
    from faststream.rabbit import RabbitMessage

    async def decoder(
        msg: RabbitMessage,
        original_decoder: Callable[[RabbitMessage], Awaitable[DecodedMessage]],
    ) -> DecodedMessage:
        return await original_decoder(msg)
    ```

=== "NATS"
    ```python
    from types import Callable, Awaitable
    from faststream.types import DecodedMessage
    from faststream.nats import NatsMessage

    async def decoder(
        msg: NatsMessage,
        original_decoder: Callable[[NatsMessage], Awaitable[DecodedMessage]],
    ) -> DecodedMessage:
        return await original_decoder(msg)
    ```

=== "Redis"
    ```python
    from types import Callable, Awaitable
    from faststream.types import DecodedMessage
    from faststream.redis import RedisMessage

    async def decoder(
        msg: RedisMessage,
        original_decoder: Callable[[RedisMessage], Awaitable[DecodedMessage]],
    ) -> DecodedMessage:
        return await original_decoder(msg)
    ```


!!! note
    The original decoder is always an asynchronous function, so your custom decoder should also be asynchronous.

Afterward, you can set this custom decoder at the broker or subscriber level.

## Example

You can find examples of *Protobuf*, *Msgpack* and *Avro* serialization in the [next article](./examples.md){.internal-link}.
