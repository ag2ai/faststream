---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Msgspec Serialization

[msgspec](https://jcristharif.com/msgspec/index.html){.external-link target="_blank"} is a *fast* serialization and validation library, with builtin
support for [JSON](https://www.json.org/json-en.html){target="_blank"}, [MessagePack](https://msgpack.org/){target="_blank"}, [YAML](https://yaml.org/){target="_blank"}, and [TOML](https://toml.io/en/){target="_blank"}. It features:

- 🚀 **High performance encoders/decoders** for common protocols. The JSON and
  MessagePack implementations regularly [`benchmark`](https://jcristharif.com/msgspec/benchmarks.html){target="_blank"} as the
  fastest options for Python.

- 🎉 **Support for a wide variety of Python types**. Additional types may
  be supported through [`extending`](https://jcristharif.com/msgspec/extending.html){target="_blank"}.

- 🔍 **Zero-cost schema validation** using familiar Python type annotations.
  In [`benchmarks`](https://jcristharif.com/msgspec/benchmarks.html){target="_blank"} `msgspec` decodes *and* validates JSON
  faster than [orjson](https://github.com/ijl/orjson){target="_blank"} can decode it alone.

- ✨ **A speedy Struct type** for representing structured data. If you already
  use [dataclasses](https://docs.python.org/3/library/dataclasses.html){target="_blank"} or [attrs](https://www.attrs.org/en/stable/){target="_blank"}, [`Structs`](https://jcristharif.com/msgspec/structs.html){target="_blank"} should feel familiar. However,
  they're [`5-60x`](https://jcristharif.com/msgspec/benchmarks.html#struct-benchmark){target="_blank"} faster for common operations.

**FastStream** supports **msgspec** as an alternative backend for serialization, which can be used instead of **Pydantic**.

To use it, you need to pass a `MsgSpecSerializer` object to the broker:

=== "AIOKafka"
    ```python
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.kafka import KafkaBroker

    broker = KafkaBroker(serializer=MsgSpecSerializer())
    ```

=== "Confluent"
    ```python
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.confluent import KafkaBroker

    broker = KafkaBroker(serializer=MsgSpecSerializer())
    ```

=== "RabbitMQ"
    ```python
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.rabbit import RabbitBroker

    broker = RabbitBroker(serializer=MsgSpecSerializer())
    ```

=== "NATS"
    ```python
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.nats import NatsBroker

    broker = NatsBroker(serializer=MsgSpecSerializer())
    ```

=== "Redis"
    ```python
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.redis import RedisBroker

    broker = RedisBroker(serializer=MsgSpecSerializer())
    ```

## msgspec.field

Msgspec [**field**](https://jcristharif.com/msgspec/api.html#msgspec.field){.external-link target="_blank"} allows you to override the field name for encoding/decoding and provide default values.

Just use `msgspec.field` as a function default argument:

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 17"
    {!> docs_src/getting_started/subscription/kafka/msgspec_fields.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 17"
    {!> docs_src/getting_started/subscription/confluent/msgspec_fields.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 17"
    {!> docs_src/getting_started/subscription/rabbit/msgspec_fields.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 17"
    {!> docs_src/getting_started/subscription/nats/msgspec_fields.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 17"
    {!> docs_src/getting_started/subscription/redis/msgspec_fields.py !}
    ```

## msgspec.Struct

To make your message schema reusable between different subscribers and publishers, you can declare it as a `msgspec.Struct` and use it as a single message annotation:

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 14-16 21"
    {!> docs_src/getting_started/subscription/kafka/msgspec_struct.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 14-16 21"
    {!> docs_src/getting_started/subscription/confluent/msgspec_struct.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 14-16 21"
    {!> docs_src/getting_started/subscription/rabbit/msgspec_struct.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 14-16 21"
    {!> docs_src/getting_started/subscription/nats/msgspec_struct.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 14-16 21"
    {!> docs_src/getting_started/subscription/redis/msgspec_struct.py !}
    ```
