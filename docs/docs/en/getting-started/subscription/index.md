---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Subscription Basics

**FastStream** provides a Message Broker agnostic way to subscribe to event streams.

You need not even know about topics/queues/subjects or any broker inner objects you use.
The basic syntax is the same for all brokers:

=== "AIOKafka"
    ```python
    from faststream.kafka import KafkaBroker

    broker = KafkaBroker()

    @broker.subscriber("test")  # topic name
    async def handle_msg(msg_body):
        ...
    ```

=== "Confluent"
    ```python
    from faststream.confluent import KafkaBroker

    broker = KafkaBroker()

    @broker.subscriber("test")  # topic name
    async def handle_msg(msg_body):
        ...
    ```

=== "RabbitMQ"
    ```python
    from faststream.rabbit import RabbitBroker

    broker = RabbitBroker()

    @broker.subscriber("test")  # queue name
    async def handle_msg(msg_body):
        ...
    ```

=== "NATS"
    ```python
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    @broker.subscriber("test")  # subject name
    async def handle_msg(msg_body):
        ...
    ```

=== "Redis"
    ```python
    from faststream.redis import RedisBroker

    broker = RedisBroker()

    @broker.subscriber("test")  # channel name
    async def handle_msg(msg_body):
        ...
    ```

!!! tip
    If you want to use Message Broker specific features, please visit the corresponding broker documentation section.
    In the **Tutorial** section, the general features are described.

Also, synchronous functions are supported as well:

=== "AIOKafka"
    ```python
    from faststream.kafka import KafkaBroker

    broker = KafkaBroker()

    @broker.subscriber("test")  # topic name
    def handle_msg(msg_body):
        ...
    ```

=== "Confluent"
    ```python
    from faststream.confluent import KafkaBroker

    broker = KafkaBroker()

    @broker.subscriber("test")  # topic name
    def handle_msg(msg_body):
        ...
    ```

=== "RabbitMQ"
    ```python
    from faststream.rabbit import RabbitBroker

    broker = RabbitBroker()

    @broker.subscriber("test")  # queue name
    def handle_msg(msg_body):
        ...
    ```

=== "NATS"
    ```python
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    @broker.subscriber("test")  # subject name
    def handle_msg(msg_body):
        ...
    ```

=== "Redis"
    ```python
    from faststream.redis import RedisBroker

    broker = RedisBroker()

    @broker.subscriber("test")  # channel name
    def handle_msg(msg_body):
        ...
    ```

## Message Body Serialization

Generally, **FastStream** uses your function type annotation to serialize incoming message body with [**Pydantic**](https://docs.pydantic.dev){.external-link target="_blank"}. This is similar to how [**FastAPI**](https://fastapi.tiangolo.com){.external-link target="_blank"} works (if you are familiar with it).

```python hl_lines="3"
@broker.subscriber("test")
async def handle_str(
    msg_body: str,
):
    ...
```

You can also access some extra features through the function arguments, such as [Depends](../dependencies/index.md){.internal-link} and [Context](../context/existed.md){.internal-link} if required.

However, you can easily disable **Pydantic** validation by creating a broker with the following option `#!python Broker(apply_types=False)`

This way **FastStream** still consumes `#!python json.loads` result, but without pydantic validation and casting.

=== "AIOKafka"
    ```python hl_lines="3"
    from faststream.kafka import KafkaBroker

    broker = KafkaBroker(apply_types=False)

    @broker.subscriber("test")
    async def handle_msg(msg_body: str):  # just an annotation, has no real effect
        ...
    ```

=== "Confluent"
    ```python hl_lines="3"
    from faststream.confluent import KafkaBroker

    broker = KafkaBroker(apply_types=False)

    @broker.subscriber("test")
    async def handle_msg(msg_body: str):  # just an annotation, has no real effect
        ...
    ```

=== "RabbitMQ"
    ```python hl_lines="3"
    from faststream.rabbit import RabbitBroker

    broker = RabbitBroker(apply_types=False)

    @broker.subscriber("test")
    async def handle_msg(msg_body: str):  # just an annotation, has no real effect
        ...
    ```

=== "NATS"
    ```python hl_lines="3"
    from faststream.nats import NatsBroker

    broker = NatsBroker(apply_types=False)

    @broker.subscriber("test")
    async def handle_msg(msg_body: str):  # just an annotation, has no real effect
        ...
    ```

=== "Redis"
    ```python hl_lines="3"
    from faststream.redis import RedisBroker

    broker = RedisBroker(apply_types=False)

    @broker.subscriber("test")
    async def handle_msg(msg_body: str):  # just an annotation, has no real effect
        ...
    ```

!!! warning
    Setting the `apply_types=False` flag not only disables type casting but also `Depends` and `Context`.
    If you want to disable only type casting, use `serializer=None` instead.

## Multiple Subscriptions

You can also subscribe to multiple event streams at the same time with one function. Just wrap it with multiple `#!python @broker.subscriber(...)` decorators (they have no effect on each other).

```python hl_lines="1-2"
@broker.subscriber("first_sub")
@broker.subscriber("second_sub")
async def handler(msg):
    ...
```
