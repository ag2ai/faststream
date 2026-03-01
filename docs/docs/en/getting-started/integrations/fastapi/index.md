---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# **FastAPI** Plugin

## Handling messages

**FastStream** can be used as a part of **FastAPI**.

Just import a **StreamRouter** you need and declare the message handler in the same way as with a regular **FastStream** application.

!!! tip
    When used in this way, **FastStream** does not use its own dependency system but integrates into **FastAPI**.
    That is, you can use `Depends`, `BackgroundTasks` and other original **FastAPI** features as if it were a regular HTTP endpoint, but you can't use `faststream.Context` and `faststream.Depends`.

    Note that the code below uses `fastapi.Depends`, not `faststream.Depends`.

    Also, instead original `faststream.Context` you should use `faststream.[broker].fastapi.Context` (the same with [already created annotations](../../context.md#annotated-aliases){.internal-link})

=== "AIOKafka"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/kafka/base.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/confluent/base.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/rabbit/base.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/nats/base.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/redis/base.py !}
    ```


!!! warning
    If you are using **fastapi < 0.112.2** version, you should setup lifespan manually `#!python FastAPI(lifespan=router.lifespan_context)`

When processing a message from a broker, the entire message body is placed simultaneously in both the `body` and `path` request parameters. You can access them in any way convenient for you. The message header is placed in `headers`.

Also, this router can be fully used as an `HttpRouter` (of which it is the inheritor). So, you can
use it to declare any `get`, `post`, `put` and other HTTP methods. For example, this is done at [**line 20**](#__codelineno-0-20).

!!! warning
    If your **ASGI** server does not support installing **state** inside **lifespan**, you can disable this behavior as follows:

    ```python
    router = StreamRouter(..., setup_state=False)
    ```

    However, after that, you will not be able to access the broker from your application's **state** (but it is still available as the `router.broker`).

## Accessing the Broker Object

The recommended way to access the broker is through the Context feature:

=== "AIOKafka"
    ``` python
    from faststream.kafka.fastapi import KafkaBroker

    @router.get("/")
    async def handler(broker: KafkaBroker): ...
    ```

=== "Confluent"
    ``` python
    from faststream.confluent.fastapi import KafkaBroker

    @router.get("/")
    async def handler(broker: KafkaBroker): ...
    ```

=== "RabbitMQ"
    ``` python hl_lines="8"
    from faststream.rabbit.fastapi import RabbitBroker

    @router.get("/")
    async def handler(broker: RabbitBroker): ...
    ```

=== "NATS"
    ``` python
    from faststream.nats.fastapi import NatsBroker

    @router.get("/")
    async def handler(broker: NatsBroker): ...
    ```

=== "Redis"
    ``` python
    from faststream.redis.fastapi import RedisBroker

    @router.get("/")
    async def handler(broker: RedisBroker): ...
    ```

However, there are a few alternative methods you can use if you prefer.

Inside each router, there is a broker. You can easily access it if you need to send a message to MQ:

=== "AIOKafka"
    ```python linenums="1" hl_lines="12"
    {!> docs_src/integrations/fastapi/kafka/send.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="12"
    {!> docs_src/integrations/fastapi/confluent/send.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="12"
    {!> docs_src/integrations/fastapi/rabbit/send.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="12"
    {!> docs_src/integrations/fastapi/nats/send.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="12"
    {!> docs_src/integrations/fastapi/redis/send.py !}
    ```


Also, you can use the following `Depends` to access the broker if you want to use it at different parts of your program:

=== "AIOKafka"
    ```python linenums="1" hl_lines="11-12 16-17"
    {!> docs_src/integrations/fastapi/kafka/depends.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="11-12 16-17"
    {!> docs_src/integrations/fastapi/confluent/depends.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="11-12 16-17"
    {!> docs_src/integrations/fastapi/rabbit/depends.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="11-12 16-17"
    {!> docs_src/integrations/fastapi/nats/depends.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="11-12 16-17"
    {!> docs_src/integrations/fastapi/redis/depends.py !}
    ```

Or you can access the broker from a **FastAPI** application state (if you don't disable it with `#!python setup_state=False`):

```python
from fastapi import Request

@app.get("/")
def main(request: Request):
    broker = request.state.broker
```

## `@after_startup`

The `FastStream` application has the `#!python @after_startup` hook, which allows you to perform operations with your message broker after the connection is established. This can be extremely convenient for managing your brokers' objects and/or sending messages. This hook is also available for your **FastAPI StreamRouter**

=== "AIOKafka"
    ```python linenums="1" hl_lines="13-15"
    {!> docs_src/integrations/fastapi/kafka/startup.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="13-15"
    {!> docs_src/integrations/fastapi/confluent/startup.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="13-15"
    {!> docs_src/integrations/fastapi/rabbit/startup.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="13-15"
    {!> docs_src/integrations/fastapi/nats/startup.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="13-15"
    {!> docs_src/integrations/fastapi/redis/startup.py !}
    ```

## Documentation

When using **FastStream** as a router for **FastAPI**, the framework automatically registers endpoints for hosting **AsyncAPI** documentation into your application with the following default values:

=== "AIOKafka"

    ```python
    from faststream.kafka.fastapi import KafkaRouter

    router = KafkaRouter(
        ...,
        schema_url="/asyncapi",
        include_in_schema=True,
    )
    ```

=== "Confluent"

    ```python
    from faststream.confluent.fastapi import KafkaRouter

    router = KafkaRouter(
        ...,
        schema_url="/asyncapi",
        include_in_schema=True,
    )
    ```

=== "RabbitMQ"

    ```python
    from faststream.rabbit.fastapi import RabbitRouter

    router = RabbitRouter(
        ...,
        schema_url="/asyncapi",
        include_in_schema=True,
    )
    ```

=== "NATS"

    ```python
    from faststream.nats.fastapi import NatsRouter

    router = NatsRouter(
        ...,
        schema_url="/asyncapi",
        include_in_schema=True,
    )
    ```

=== "Redis"

    ```python
    from faststream.redis.fastapi import RedisRouter

    router = RedisRouter(
        ...,
        schema_url="/asyncapi",
        include_in_schema=True,
    )
    ```

This way, you will have three routes to interact with your application's **AsyncAPI** schema:

* `/asyncapi` - the same as the [CLI created page](../../../getting-started/asyncapi/hosting.md){.internal-link}
* `/asyncapi.json` - download the **JSON** schema representation
* `/asyncapi.yaml` - download the **YAML** schema representation

## Testing

To test your **FastAPI StreamRouter**, you can still use it with the *TestClient*:

=== "AIOKafka"
    ```python linenums="1" hl_lines="3 5 13-16"
    {!> docs_src/integrations/fastapi/kafka/test.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3 5 13-16"
    {!> docs_src/integrations/fastapi/confluent/test.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3 5 13-16"
    {!> docs_src/integrations/fastapi/rabbit/test.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3 5 13-16"
    {!> docs_src/integrations/fastapi/nats/test.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3 5 13-16"
    {!> docs_src/integrations/fastapi/redis/test.py !}
    ```

## Multiple Routers

Using **FastStream** as a **FastAPI** plugin you are still able to separate messages processing logic between different routers (like with a regular `HTTPRouter`). But it can be confusing - **StreamRouter** patches a **FastAPI** object lifespan.

Fortunately, you can use regular **FastStream** routers and include them to the **FastAPI** integration one like in the regular broker object. Also, it can be useful to reuse your endpoints between **FastAPI** integration and regular **FastStream** app.

=== "AIOKafka"
    ```python linenums="1" hl_lines="2-3 6 12-14 16"
    {!> docs_src/integrations/fastapi/kafka/router.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="2-3 6 12-14 16"
    {!> docs_src/integrations/fastapi/confluent/router.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="2-3 6 12-14 16"
    {!> docs_src/integrations/fastapi/rabbit/router.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="2-3 6 12-14 16"
    {!> docs_src/integrations/fastapi/nats/router.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="2-3 6 12-14 16"
    {!> docs_src/integrations/fastapi/redis/router.py !}
    ```
