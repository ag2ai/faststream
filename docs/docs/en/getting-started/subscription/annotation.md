---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Annotation Serialization

## Basic usage

As you already know, **FastStream** serializes your incoming message body according to the function type annotations using [**Pydantic**](https://docs.pydantic.dev){.external-link target="_blank"}.

So, there are some valid use cases:

```python hl_lines="3 9 15"
@broker.subscriber("test")
async def handle(
    msg: str,
):
    ...

@broker.subscriber("test")
async def handle(
    msg: bytes,
):
    ...

@broker.subscriber("test")
async def handle(
    msg: int,
):
    ...
```


As with other Python primitive types as well (`#!python float`, `#!python bool`, `#!python datetime`, etc)

!!! note
    If the incoming message cannot be serialized by the described schema, **FastStream** raises a `pydantic.ValidationError` with a correct log message.

Also, thanks to **Pydantic** (again), **FastStream** is able to serialize (and validate) more complex types like `pydantic.HttpUrl`, `pydantic.PositiveInt`, etc.

## JSON Basic Serialization

But how can we serialize more complex message, like `#!json { "name": "John", "user_id": 1 }` ?

For sure, we can serialize it as a simple `#!python dict`

```python hl_lines="5"
from typing import Dict, Any

@broker.subscriber("test")
async def handle(
    msg: dict[str, Any],
):
    ...
```


But it doesn't looks like a correct message validation, does it?

For this reason, **FastStream** supports per-argument message serialization: you can declare multiple arguments with various types and your message will unpack to them:

=== "AIOKafka"
    ```python hl_lines="3-4"
    {!> docs_src/getting_started/subscription/kafka/annotation.py [ln:8-14] !}
    ```

=== "Confluent"
    ```python hl_lines="3-4"
    {!> docs_src/getting_started/subscription/confluent/annotation.py [ln:8-14] !}
    ```

=== "RabbitMQ"
    ```python hl_lines="3-4"
    {!> docs_src/getting_started/subscription/rabbit/annotation.py [ln:8-14] !}
    ```

=== "NATS"
    ```python hl_lines="3-4"
    {!> docs_src/getting_started/subscription/nats/annotation.py [ln:8-14] !}
    ```

=== "Redis"
    ```python hl_lines="3-4"
    {!> docs_src/getting_started/subscription/redis/annotation.py [ln:8-14] !}
    ```


!!! tip
    By default **FastStream** uses `json.loads` to decode and `json.dumps` to encode your messages. But if you prefer [**orjson**](https://github.com/ijl/orjson){.external-link target="_blank"}, just install it and framework will use it automatically.
