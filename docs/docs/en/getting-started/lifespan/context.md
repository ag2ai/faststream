---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Lifespan Context Manager

Also, you can define *startup* and *shutdown* logic using the `lifespan` parameter of the **FastStream** app, and a "context manager" (I'll show you what that is in a second).

Let's start with the example from the [hooks page](./hooks.md#another-example){.internal-link} and refactor it using "context manager".

We create an async function `lifespan()` with `#!python yield` like this:

=== "AIOKafka"
    ```python linenums="1" hl_lines="16-17 22"
    {!> docs_src/getting_started/lifespan/kafka/ml_context.py!}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="16-17 22"
    {!> docs_src/getting_started/lifespan/confluent/ml_context.py!}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="16-17 22"
    {!> docs_src/getting_started/lifespan/rabbit/ml_context.py!}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="16-17 22"
    {!> docs_src/getting_started/lifespan/nats/ml_context.py!}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="16-17 22"
    {!> docs_src/getting_started/lifespan/redis/ml_context.py!}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="16-17 22"
    {!> docs_src/getting_started/lifespan/mqtt/ml_context.py!}
    ```

As you can see, the `lifespan` parameter is much more suitable for the case (than separate `#!python @app.on_startup` and `#!python @app.after_shutdown` calls) if you have an object that needs to be processed at both application startup and shutdown.

!!! tip
    `lifespan` starts **BEFORE** your broker is started (`#!python @app.on_startup` hook) and finishes **AFTER** the broker is shut down (`#!python @app.after_shutdown`), so you can't publish any messages here.

    If you want to perform some actions with an *already/still running broker*, please use `#!python @app.after_startup` and `#!python @app.on_shutdown` hooks.

Also, `lifespan` supports all **FastStream** hooks features:

* Dependency Injection
* [extra **CLI**](../cli.md){.internal-link} options passing
