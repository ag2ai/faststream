---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Context Fields Declaration

You can also store your own objects in the `Context`.

## Global

To declare an application-level context field, you need to call the `context.set_global` method with a key to indicate where the object will be placed in the context.

=== "AIOKafka"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/kafka/custom_global_context.py [ln:1-5,13-16] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/confluent/custom_global_context.py [ln:1-5,13-16] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/rabbit/custom_global_context.py [ln:1-5,13-16] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/nats/custom_global_context.py [ln:1-5,13-16] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/redis/custom_global_context.py [ln:1-5,13-16] !}
    ```

Afterward, you can access your `secret` field in the usual way:

=== "AIOKafka"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/kafka/custom_global_context.py [ln:8-13] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/confluent/custom_global_context.py [ln:8-13] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/rabbit/custom_global_context.py [ln:8-13] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/nats/custom_global_context.py [ln:8-13] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/redis/custom_global_context.py [ln:8-13] !}
    ```

In this case, the field becomes a global context field: it does not depend on the current message handler (unlike `message`)

!!! tip
    Alternatively you can setup global context objects in `FastStream` object constructor:

    ```python
    from faststream import FastStream
    from faststream.context import ContextRepo

    app = FastStream(context=ContextRepo({
        "secret_str": "my-perfect-secret"
    }))
    ```

To remove a field from the context use the `reset_global` method:

```python
context.reset_global("my_key")
```

## Local

To set a local context (available only within the message processing scope), use the context manager `scope`. It could me extremely uselful to fill context with additional options in [Middlewares](../middlewares/){.internal-link}

=== "AIOKafka"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/kafka/custom_local_context.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/confluent/custom_local_context.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/rabbit/custom_local_context.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/nats/custom_local_context.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/redis/custom_local_context.py !}
    ```

## Existing Fields

**Context** already contains some global objects that you can always access:

* **broker** - the current broker
* **context** - the context itself, in which you can write your own fields
* **logger** - the logger used for your broker (tags messages with *message_id*)
* **message** - the raw message (if you need access to it)

At the same time, thanks to `contextlib.ContextVar`, **message** is local for you current consumer scope.

### Access to Context Fields

By default, the context searches for an object based on the argument name.

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 8-11"
    {!> docs_src/getting_started/context/kafka/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 8-11"
    {!> docs_src/getting_started/context/confluent/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 8-11"
    {!> docs_src/getting_started/context/rabbit/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 8-11"
    {!> docs_src/getting_started/context/nats/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 8-11"
    {!> docs_src/getting_started/context/redis/existed_context.py [ln:1-2,9-12,14-23] !}
    ```

### Annotated Aliases

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

    ```python linenums="1" hl_lines="3-8 15-18"
    {!> docs_src/getting_started/context/kafka/existed_context.py [ln:1-11,22-31] !}
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

    ```python linenums="1" hl_lines="3-8 15-18"
    {!> docs_src/getting_started/context/confluent/existed_context.py [ln:1-11,22-31] !}
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

    ```python linenums="1" hl_lines="3-8 15-18"
    {!> docs_src/getting_started/context/rabbit/existed_context.py [ln:1-11,22-31] !}
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

    ```python linenums="1" hl_lines="3-8 15-18"
    {!> docs_src/getting_started/context/nats/existed_context.py [ln:1-11,22-31] !}
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

    ```python linenums="1" hl_lines="3-8 15-18"
    {!> docs_src/getting_started/context/redis/existed_context.py [ln:1-11,22-31] !}
    ```

## Context Extra Options

Additionally, `Context` provides you with some extra capabilities for working with containing objects.

### Default Values

For instance, if you attempt to access a field that doesn't exist in the global context, you will receive a `pydantic.ValidationError` exception.

However, you can set default values if needed.

=== "AIOKafka"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/kafka/default_arguments.py [ln:7-11] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/confluent/default_arguments.py [ln:7-11] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/rabbit/default_arguments.py [ln:7-11] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/nats/default_arguments.py [ln:7-11] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/redis/default_arguments.py [ln:7-11] !}
    ```

### Cast Context Types

By default, context fields are **NOT CAST** to the type specified in their annotation.

=== "AIOKafka"
    ```python linenums="1" hl_lines="8 13 15"
    {!> docs_src/getting_started/context/kafka/cast.py [ln:1-15] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="8 13 15"
    {!> docs_src/getting_started/context/confluent/cast.py [ln:1-15] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="8 13 15"
    {!> docs_src/getting_started/context/rabbit/cast.py [ln:1-15] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="8 13 15"
    {!> docs_src/getting_started/context/nats/cast.py [ln:1-15] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="8 13 15"
    {!> docs_src/getting_started/context/redis/cast.py [ln:1-15] !}
    ```

If you require this functionality, you can enable the appropriate flag.

=== "AIOKafka"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/kafka/cast.py [ln:16-21] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/confluent/cast.py [ln:16-21] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/rabbit/cast.py [ln:16-21] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/nats/cast.py [ln:16-21] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/redis/cast.py [ln:16-21] !}
    ```

### Initial Value

Also, `Context` provides you with a `initial` option to setup base context value without previous `set_global` call.

=== "AIOKafka"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/kafka/initial.py [ln:7-12] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/confluent/initial.py [ln:7-12] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/rabbit/initial.py [ln:7-12] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/nats/initial.py [ln:7-12] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/redis/initial.py [ln:7-12] !}
    ```

## Access by Name

Sometimes, you may need to use a different name for the argument (not the one under which it is stored in the context) or get access to specific parts of the object. To do this, simply specify the name of what you want to access, and the context will provide you with the object.

=== "AIOKafka"
    ```python linenums="1" hl_lines="11-12"
    {!> docs_src/getting_started/context/kafka/fields_access.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="11-12"
    {!> docs_src/getting_started/context/confluent/fields_access.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="11-12"
    {!> docs_src/getting_started/context/rabbit/fields_access.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="11-12"
    {!> docs_src/getting_started/context/nats/fields_access.py !}
    ```


=== "Redis"
    ```python linenums="1" hl_lines="11-12"
    {!> docs_src/getting_started/context/redis/fields_access.py !}
    ```

This way you can get access to context object specific field


```python
{! docs_src/getting_started/context/kafka/fields_access.py [ln:11] !}
```

Or even to a dict key


```python
{! docs_src/getting_started/context/kafka/fields_access.py [ln:12] !}
```

## Application Context

**FastStreams** has its own Dependency Injection container - **Context**, used to store application runtime objects and variables.

With this container, you can access both application scope and message processing scope objects. This functionality is similar to [`Depends`](../dependencies/index.md){.internal-link} usage.

=== "AIOKafka"
    ```python linenums="1" hl_lines="2 4 12"
    {!> docs_src/getting_started/context/kafka/annotated.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="2 4 12"
    {!> docs_src/getting_started/context/confluent/annotated.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="2 4 12"
    {!> docs_src/getting_started/context/rabbit/annotated.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="2 4 12"
    {!> docs_src/getting_started/context/nats/annotated.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="2 4 12"
    {!> docs_src/getting_started/context/redis/annotated.py !}
    ```

### Usages

By default, the context is available in the same place as `Depends`:

* at lifespan hooks
* message subscribers
* nested dependencies

!!! tip
    You can get access to the **Context** in [Middlewares](../middlewares/#context-access){.internal-link} as `#!python self.context`
