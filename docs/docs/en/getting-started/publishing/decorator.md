---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publisher Decorator

The second easiest way to publish messages is by using the Publisher Decorator. This method has an [**AsyncAPI**](../asyncapi/custom.md){.internal-link} representation and is suitable for quickly creating applications. However, it doesn't provide all testing features.

It creates a structured DataPipeline unit with an input and output. The order of Subscriber and Publisher decorators doesn't matter, but `#!python @broker.publisher(...)` can be used only with functions already decorated by a `#!python @broker.subscriber(...)`.

!!! note
    It uses the handler function's return type annotation to cast the function's return value before sending, so be accurate with it.

!!! tip "Pros and Cons"

    :material-checkbox-marked:{.checked_mark} **Easy to use** - Publishing messages in **FastStream** is intuitive and requires minimal effort.

    :material-checkbox-marked:{.checked_mark} **AsyncAPI support** - [`AsyncAPI`](../asyncapi/export.md) is a specification for describing asynchronous APIs used in messaging applications. This method supports the specification render.

    :fontawesome-solid-square-xmark:{.x_mark} **No testing support** - This method lacks full [`Testing`](./test.md) support.

    :fontawesome-solid-square-xmark:{.x_mark} **No broker availability from Context** - You cannot leverage **FastStream's** [`Context`](../context/index.md), a built-in Dependency Injection (DI) container, to work with brokers or other external services.

    :fontawesome-solid-square-xmark:{.x_mark} **Not reusable** - This method cannot be reused.

    :fontawesome-solid-triangle-exclamation:{.warning_mark} The message will **always** be published.

=== "AIOKafka"
    ```python linenums="1" hl_lines="9"
    {!> docs_src/getting_started/publishing/kafka/decorator.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="9"
    {!> docs_src/getting_started/publishing/confluent/decorator.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="9"
    {!> docs_src/getting_started/publishing/rabbit/decorator.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="9"
    {!> docs_src/getting_started/publishing/nats/decorator.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="9"
    {!> docs_src/getting_started/publishing/redis/decorator.py !}
    ```

`#!python @broker.publisher(...)` properly sets the same `correlation_id` as the incoming message. This way, you get the same `correlation_id` for the entire message pipeline across all services, allowing you to collect a trace.

## Message Broadcasting

The decorator can be used multiple times with one function to broadcast the function's return:

```python hl_lines="2-3"
@broker.subscriber("in")
@broker.publisher("first-out")
@broker.publisher("second-out")
async def handle(msg) -> str:
    return "Response"
```

This way you will send a copy of your return to the all output topics.

!!! note
    Also, if this subscriber consumes a message with **RPC** mode, it sends a reply not only to the **RPC** channel but also to all publishers as well.

    See more information about RPC in the specific broker sections:

    * [Kafka RPC](../../howto/kafka/rpc.md)
    * [RabbitMQ RPC](../../rabbit/rpc.md)
    * [Nats PRC](../../nats/rpc.md)
    * [Redis RPC](../../redis/rpc.md)
