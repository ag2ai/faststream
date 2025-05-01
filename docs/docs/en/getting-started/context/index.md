---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Application Context

**FastStreams** has its own Dependency Injection container - **Context**, used to store application runtime objects and variables.

With this container, you can access both application scope and message processing scope objects. This functionality is similar to [`Depends`](../dependencies/index.md){.internal-link} usage.

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 11"
    {!> docs_src/getting_started/context/kafka/base.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 11"
    {!> docs_src/getting_started/context/confluent/base.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 11"
    {!> docs_src/getting_started/context/rabbit/base.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 11"
    {!> docs_src/getting_started/context/nats/base.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 11"
    {!> docs_src/getting_started/context/redis/base.py !}
    ```

But, with the [`Annotated`](https://docs.python.org/3/library/typing.html#typing.Annotated){.external-docs target="_blank"} Python feature usage, it is much closer to `#!python @pytest.fixture`.

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 7 16"
    {!> docs_src/getting_started/context/kafka/annotated.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 7 16"
    {!> docs_src/getting_started/context/confluent/annotated.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 7 16"
    {!> docs_src/getting_started/context/rabbit/annotated.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 7 16"
    {!> docs_src/getting_started/context/nats/annotated.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 7 16"
    {!> docs_src/getting_started/context/redis/annotated.py !}
    ```

## Usages

By default, the context is available in the same place as `Depends`:

* at lifespan hooks
* message subscribers
* nested dependencies

!!! tip
    Fields obtained from the `Context` are editable, so editing them in a function means editing them everywhere.

## Compatibility with Regular Functions

To use context in other functions, use the `#!python @apply_types` decorator. In this case, the context of the called function will correspond to the context of the event handler from which it was called.

```python linenums="1" hl_lines="5 7-8"
{! docs_src/getting_started/context/nested.py [ln:1-2,9-12,14-16] !}
```

In the example above, we did not pass the `logger` function at calling it; it was placed from context.
