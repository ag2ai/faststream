---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publisher Direct Usage

The Publisher Object provides a full-featured way to publish messages. It has an [**AsyncAPI**](../asyncapi/custom.md){.internal-link} representation and includes [testability](./test.md){.internal-link} features.

This method creates a reusable Publisher object that can be used directly to publish a message:

!!! tip "Pros and Cons"

    :material-checkbox-marked:{.checked_mark} **AsyncAPI support** - [`AsyncAPI`](../asyncapi/export.md) is a specification for describing asynchronous APIs used in messaging applications. This method supports the specification render.

    :material-checkbox-marked:{.checked_mark} **Testing support** - This method has full [`Testing`](./test.md) support.

    :material-checkbox-marked:{.checked_mark} **Broker availability from Context** - You can leverage **FastStream's** [`Context`](../context.md), a built-in Dependency Injection (DI) container, to work with brokers or other external services.

    :material-checkbox-marked:{.checked_mark} **Optional publication** - You can create optional publications.

    :material-checkbox-marked:{.checked_mark} **Reusable** - This method is reusable.

=== "AIOKafka"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/kafka/direct.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/confluent/direct.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/rabbit/direct.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/nats/direct.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/redis/direct.py !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/mqtt/direct.py !}
    ```

It is something in the middle between [broker publish](./broker.md){.internal-link} and [object decorator](./object.md){.internal-link}. It has an **AsyncAPI** representation and *testability* features (like the **object decorator**), but allows you to send different messages to different outputs (like the **broker publish**).

```python hl_lines="3-4"
@broker.subscriber("in")
async def handle(msg) -> str:
    await publisher1.publish("Response-1")
    await publisher2.publish("Response-2")
```

### Overriding the Destination

The destination passed to `#!python broker.publisher(...)` is only a default. Every `#!python publisher.publish(...)` call accepts the same destination arguments as `#!python broker.publish(...)`, and a value passed there **overrides** the one from the constructor for that single call:

=== "AIOKafka"
    ```python hl_lines="5"
    publisher = broker.publisher("another-topic")

    @broker.subscriber("in")
    async def handle(msg: str) -> None:
        await publisher.publish(msg, topic=f"another-topic.{msg}")
    ```

=== "Confluent"
    ```python hl_lines="5"
    publisher = broker.publisher("another-topic")

    @broker.subscriber("in")
    async def handle(msg: str) -> None:
        await publisher.publish(msg, topic=f"another-topic.{msg}")
    ```

=== "RabbitMQ"
    ```python hl_lines="5"
    publisher = broker.publisher("another-queue")

    @broker.subscriber("in")
    async def handle(msg: str) -> None:
        await publisher.publish(msg, routing_key=f"another-queue.{msg}")
    ```

=== "NATS"
    ```python hl_lines="5"
    publisher = broker.publisher("another-subject")

    @broker.subscriber("in")
    async def handle(msg: str) -> None:
        await publisher.publish(msg, subject=f"another-subject.{msg}")
    ```

=== "Redis"
    ```python hl_lines="5"
    publisher = broker.publisher("another-channel")

    @broker.subscriber("in")
    async def handle(msg: str) -> None:
        await publisher.publish(msg, channel=f"another-channel.{msg}")
    ```

=== "MQTT"
    ```python hl_lines="5"
    publisher = broker.publisher("another-topic")

    @broker.subscriber("in")
    async def handle(msg: str) -> None:
        await publisher.publish(msg, topic=f"another-topic/{msg}")
    ```

The constructor argument still matters: the **AsyncAPI** schema describes the publisher by it, and the [test mock](./test.md){.internal-link} is attached to it. If a publisher never publishes to its default destination, it is probably [broker publish](./broker.md){.internal-link} you want.

!!! note
    When using this method, **FastStream** doesn't reuse the incoming `correlation_id` to mark outgoing messages with it. You should set it manually if it is required:


    === "AIOKafka"
        ``` python linenums="3" hl_lines="3"
        @broker.subscriber("test-queue")
        async def handle(message: KafkaMessage):
            await publisher.publish("Hi!", correlation_id=message.correlation_id)
        ```

    === "Confluent"
        ``` python linenums="3" hl_lines="3"
        @broker.subscriber("test-queue")
        async def handle(message: KafkaMessage):
            await publisher.publish("Hi!", correlation_id=message.correlation_id)
        ```

    === "RabbitMQ"
        ``` python linenums="3" hl_lines="3"
        @broker.subscriber("test-queue")
        async def handle(message: RabbitMessage):
            await publisher.publish("Hi!", correlation_id=message.correlation_id)
        ```

    === "NATS"
        ``` python linenums="3" hl_lines="3"
        @broker.subscriber("test-queue")
        async def handle(message: NatsMessage):
            await publisher.publish("Hi!", correlation_id=message.correlation_id)
        ```

    === "Redis"
        ``` python linenums="3" hl_lines="3"
        @broker.subscriber("test-queue")
        async def handle(message: RedisMessage):
            await publisher.publish("Hi!", correlation_id=message.correlation_id)
        ```

    === "MQTT"
        ``` python linenums="3" hl_lines="3"
        @broker.subscriber("test-topic")
        async def handle(message: MQTTMessage):
            await publisher.publish("Hi!", correlation_id=message.correlation_id)
        ```
