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

Let's start with an example from [hooks page](./hooks.md#another-example){.internal-link} and refactor it using "context manager".

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

As you can see, `lifespan` parameter is much suitable for case (than `#!python @app.on_startup` and `#!python @app.after_shutdown` separated calls) if you have object needs to process at application startup and shutdown both.

!!! tip
    `lifespan` starts **BEFORE** your broker started (`#!python @app.on_startup` hook) and **AFTER** broker was shutdown (`#!python @app.after_shutdown`), so you can't publish any messages here.

    If you want to make some actions will *already/still running broker*, please use `#!python @app.after_startup` and `#!python @app.on_shutdown` hooks.

Also, `lifespan` supports all **FastStream** hooks features:

* Dependency Injection
* [extra **CLI**](../cli/index.md#environment-management){.internal-link} options passing
