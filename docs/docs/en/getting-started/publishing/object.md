---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publisher Object

The Publisher Object provides a full-featured way to publish messages. It has an [**AsyncAPI**](../asyncapi/custom.md){.internal-link} representation and includes [testability](./test.md){.internal-link} features. This method creates a reusable Publisher object.

Additionally, this object can be used as a decorator. The order of Subscriber and Publisher decorators doesn't matter, but `#!python @publisher` can be used only with functions already decorated by a `#!python @broker.subscriber(...)`.

!!! note
    It uses the handler function's return type annotation to cast the function's return value before sending, so be accurate with it.

!!! tip "Pros and Cons"

    :material-checkbox-marked:{.checked_mark} **AsyncAPI support** - [**```AsyncAPI```**](../asyncapi/export.md#section{.css-styles}) is a specification for describing asynchronous APIs used in messaging applications. This method supports this standard.

    :material-checkbox-marked:{.checked_mark} **Testing support** - This method has full [**```Testing```**](./test.md#section{.css-styles}) support.

    :material-checkbox-marked:{.checked_mark} **Broker availability from Context** - You can leverage **FastStream's** [**```Context```**](../context/index.md#section{.css-styles}), a built-in Dependency Injection (DI) container, to work with brokers or other external services.

    :material-checkbox-marked:{.checked_mark} **Reusable** - This method is reusable.

    :fontawesome-solid-triangle-exclamation:{.warning_mark} The message will **always** be published.

=== "AIOKafka"
    ```python linenums="1" hl_lines="7 9"
    {!> docs_src/getting_started/publishing/kafka/object.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="7 9"
    {!> docs_src/getting_started/publishing/confluent/object.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="7 9"
    {!> docs_src/getting_started/publishing/rabbit/object.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="7 9"
    {!> docs_src/getting_started/publishing/nats/object.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="7 9"
    {!> docs_src/getting_started/publishing/redis/object.py !}
    ```

## Message Broadcasting

The decorator can be used multiple times with one function to broadcast the function's return:

```python hl_lines="1-2"
@publisher1
@publisher2
@broker.subscriber("in")
async def handle(msg) -> str:
    return "Response"
```

This way, you will send a copy of your return to all output topics.

!!! note
    Also, if this subscriber consumes a message with **RPC** mode, it sends a reply not only to the **RPC** channel but also to all publishers as well.

## Details

Additionally, `#!python @publisher` automatically sends a message with the same `correlation_id` as the incoming message. This way, you get the same `correlation_id` for the entire message pipeline process across all services, allowing you to collect a trace.
