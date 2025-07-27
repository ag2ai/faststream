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

!!! note
    Pass `MsgSpecSerializer` to the broker to use msgspec:
    ```python
    from fast_depends.msgspec import MsgSpecSerializer

    broker = KafkaBroker("localhost:9092", serializer=MsgSpecSerializer())
    ```

## msgspec.field

Msgspec [**field**](https://jcristharif.com/msgspec/api.html#msgspec.field){.external-link target="_blank"} allows you to override the field name for encoding/decoding and provide default values.

Just use `msgspec.field` as a function default argument:

=== "AIOKafka"
    ```python linenums="1" hl_lines="9 16-17"
    {!> docs_src/getting_started/subscription/kafka/msgspec_fields.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="9 16-17"
    {!> docs_src/getting_started/subscription/confluent/msgspec_fields.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="9 16-17"
    {!> docs_src/getting_started/subscription/rabbit/msgspec_fields.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="9 16-17"
    {!> docs_src/getting_started/subscription/nats/msgspec_fields.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="9 16-17"
    {!> docs_src/getting_started/subscription/redis/msgspec_fields.py !}
    ```

## msgspec.Struct

To make your message schema reusable between different subscribers and publishers, you can declare it as a `msgspec.Struct` and use it as a single message annotation:

=== "AIOKafka"
    ```python linenums="1" hl_lines="9 14 21"
    {!> docs_src/getting_started/subscription/kafka/msgspec_struct.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="9 14 21"
    {!> docs_src/getting_started/subscription/confluent/msgspec_struct.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="9 14 21"
    {!> docs_src/getting_started/subscription/rabbit/msgspec_struct.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="9 14 21"
    {!> docs_src/getting_started/subscription/nats/msgspec_struct.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="9 14 21"
    {!> docs_src/getting_started/subscription/redis/msgspec_struct.py !}
    ```
