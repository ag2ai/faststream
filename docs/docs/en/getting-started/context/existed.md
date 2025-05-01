---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Existing Fields

**Context** already contains some global objects that you can always access:

* **broker** - the current broker
* **context** - the context itself, in which you can write your own fields
* **logger** - the logger used for your broker (tags messages with *message_id*)
* **message** - the raw message (if you need access to it)

At the same time, thanks to `contextlib.ContextVar`, **message** is local for you current consumer scope.

## Access to Context Fields

By default, the context searches for an object based on the argument name.

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 10-13"
    {!> docs_src/getting_started/context/kafka/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 10-13"
    {!> docs_src/getting_started/context/confluent/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 10-13"
    {!> docs_src/getting_started/context/rabbit/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 10-13"
    {!> docs_src/getting_started/context/nats/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 10-13"
    {!> docs_src/getting_started/context/redis/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

## Annotated Aliases

Also, **FastStream** has already created `Annotated` aliases to provide you with comfortable access to existing objects. You can import them directly from `faststream` or your broker-specific modules:

* Shared aliases

```python
from faststream import Logger, ContextRepo
```

=== "AIOKafka"
    ```python
    from faststream.kafka.annotations import (
        Logger, ContextRepo, KafkaMessage,
        KafkaBroker, KafkaProducer, NoCast,
    )
    ```

    !!! tip ""
        `faststream.kafka.KafkaMessage` is an alias to `faststream.kafka.annotations.KafkaMessage`

        ```python
        from faststream.kafka import KafkaMessage
        ```

    To use them, simply import and use them as subscriber argument annotations.

    ```python linenums="1" hl_lines="3-8 16-19"
    {!> docs_src/getting_started/context/kafka/existed_context.py [ln:1-11,25-35] !}
    ```

=== "Confluent"
    ```python
    from faststream.confluent.annotations import (
        Logger, ContextRepo, KafkaMessage,
        KafkaBroker, KafkaProducer, NoCast,
    )
    ```

    !!! tip ""
        `faststream.confluent.KafkaMessage` is an alias to `faststream.confluent.annotations.KafkaMessage`

        ```python
        from faststream.confluent import KafkaMessage
        ```

    To use them, simply import and use them as subscriber argument annotations.

    ```python linenums="1" hl_lines="3-8 16-19"
    {!> docs_src/getting_started/context/confluent/existed_context.py [ln:1-11,25-35] !}
    ```

=== "RabbitMQ"
    ```python
    from faststream.rabbit.annotations import (
        Logger, ContextRepo, RabbitMessage,
        RabbitBroker, RabbitProducer, NoCast,
    )
    ```

    !!! tip ""
        `faststream.rabbit.RabbitMessage` is an alias to `faststream.rabbit.annotations.RabbitMessage`

        ```python
        from faststream.rabbit import RabbitMessage
        ```

    To use them, simply import and use them as subscriber argument annotations.

    ```python linenums="1" hl_lines="3-8 16-19"
    {!> docs_src/getting_started/context/rabbit/existed_context.py [ln:1-11,25-35] !}
    ```

=== "NATS"
    ```python
    from faststream.nats.annotations import (
        Logger, ContextRepo, NatsMessage,
        NatsBroker, NatsProducer, NatsJsProducer,
        Client, JsClient, NoCast,
    )
    ```

    !!! tip ""
        `faststream.nats.NatsMessage` is an alias to `faststream.nats.annotations.NatsMessage`

        ```python
        from faststream.nats import NatsMessage
        ```
    To use them, simply import and use them as subscriber argument annotations.

    ```python linenums="1" hl_lines="3-8 16-19"
    {!> docs_src/getting_started/context/nats/existed_context.py [ln:1-11,25-35] !}
    ```

=== "Redis"
    ```python
    from faststream.redis.annotations import (
        Logger, ContextRepo, RedisMessage,
        RedisBroker, Redis, NoCast,
    )
    ```

    !!! tip ""
        `faststream.redis.RedisMessage` is an alias to `faststream.redis.annotations.RedisMessage`

        ```python
        from faststream.redis import RedisMessage
        ```
    To use them, simply import and use them as subscriber argument annotations.

    ```python linenums="1" hl_lines="3-8 16-19"
    {!> docs_src/getting_started/context/redis/existed_context.py [ln:1-11,25-35] !}
    ```
