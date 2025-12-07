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

```python linenums="1" hl_lines="3 9 15"
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

```python linenums="1" hl_lines="5"
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
    ```python linenums="1" hl_lines="3-4"
    {!> docs_src/getting_started/subscription/kafka/annotation.py [ln:8-14] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3-4"
    {!> docs_src/getting_started/subscription/confluent/annotation.py [ln:8-14] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3-4"
    {!> docs_src/getting_started/subscription/rabbit/annotation.py [ln:8-14] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3-4"
    {!> docs_src/getting_started/subscription/nats/annotation.py [ln:8-14] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3-4"
    {!> docs_src/getting_started/subscription/redis/annotation.py [ln:8-14] !}
    ```


!!! tip
    By default **FastStream** uses `#!python json.loads()` to decode and `#!python json.dumps()` to encode your messages. But if you prefer [**orjson**](https://github.com/ijl/orjson){.external-link target="_blank"} just install it and framework will use it automatically.

### Serialization details

#### Simple message

If you expect to consume simple message like `#!python b"1"` or `#!python b"any_string"`, using the single argument as a function annotation.

In this case your argument name has no matter cuz it is a total message body.

See the examples below:

```python linenums="1"
async def handler(body: int): ...
    # waits any int-serializable simple message like b"1"
```

```python linenums="1"
async def handler(body: str): ...
    # waits any str-serializable simple message like b"any_string"
```

#### JSON-like message

If you expect to consume a message with a specific structure like JSON, multiple arguments is a shortcut for JSONs.

In this case your message will be unpacked and serialized by various fields

See the examples below:

```python linenums="1"
async def handler(name: str, id: int): ...
    # waits for { "name": "John", "id": 1, ... }
```

To consume single JSON, you should create a single-field pydantic model and use it for annotation.

```python linenums="1"
class User(BaseModel):
    name: str

async def handler(body: User): ...
    # waits for { "name": "John" }
```


#### Partial body consuming

If you don't need to use all the fields, you can simply specify the fields you want to use, and the other will be ignored. See the example below:

```python linenums="1" hl_lines="14-18"
from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker()
app = FastStream(broker)

@broker.subscriber("test")
async def handle(name: str, age: int):
    print(f"{name=}, {age=}")

@app.after_startup
async def t():
    await broker.publish({
        "name": "John",
        "age": 25,
        "useless": {
            "nested": "useless"
        }
    }, topic="test")
```
