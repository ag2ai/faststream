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

## Usages

By default, the context is available in the same place as `Depends`:

* at lifespan hooks
* message subscribers
* nested dependencies

!!! tip
    You can get access to the **Context** in [Middlewares](../middlewares/#context-access){.internal-link} as `#!python self.context`
