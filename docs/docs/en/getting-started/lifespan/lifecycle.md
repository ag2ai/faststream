---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Application Lifecycle

`FastStream` exposes four methods that drive the application, and they are not interchangeable:

| Method    | What it does                                                                                   |
| --------- | ---------------------------------------------------------------------------------------------- |
| `run()`   | Installs signal handlers, enters the `lifespan` context, starts, blocks until asked to exit, stops. |
| `start()` | Runs the startup hooks and starts every broker. Returns as soon as they are consuming.        |
| `stop()`  | Runs the shutdown hooks and stops every broker.                                               |
| `exit()`  | Tells a running `run()` to leave its loop and shut down.                                      |

## `run()`

This is what the [CLI](../cli.md){.internal-link} calls for you: `faststream run module:app` imports the module and awaits `#!python app.run()`. You can do the same without the CLI:

```python
import asyncio

asyncio.run(app.run())
```

`run()` **owns the process lifecycle**:

1. It registers handlers for `SIGINT` and `SIGTERM` that call `#!python app.exit()`. This is why <kbd>Ctrl</kbd>+<kbd>C</kbd> results in a clean shutdown instead of a traceback.
2. It enters the `#!python FastStream(lifespan=...)` context manager, if you passed one. See [Lifespan Option](./context.md){.internal-link}.
3. It calls `start()`, then waits until `exit()` is called.
4. It calls `stop()` and leaves the `lifespan` context.

Because `run()` blocks until the process is told to stop, it should be the only thing your `main` awaits.

## `start()` and `stop()`

`start()` and `stop()` are the two halves of `run()` without the parts that assume they own the process: they don't touch signal handlers and they don't enter the `lifespan` context manager. Both still run the [lifespan hooks](./hooks.md){.internal-link}, so `on_startup` / `after_startup` and `on_shutdown` / `after_shutdown` behave exactly as under `run()`.

Use them when something else already owns the event loop: you embed **FastStream** into a service that has its own `main`, run it next to another framework, or drive several applications from one place. Your code decides when to stop, and it is your code that reacts to signals:

=== "AIOKafka"
    ```python linenums="1" hl_lines="17 21 28-29 31"
    {!> docs_src/getting_started/lifespan/kafka/manual_run.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="17 21 28-29 31"
    {!> docs_src/getting_started/lifespan/confluent/manual_run.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="17 21 28-29 31"
    {!> docs_src/getting_started/lifespan/rabbit/manual_run.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="17 21 28-29 31"
    {!> docs_src/getting_started/lifespan/nats/manual_run.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="17 21 28-29 31"
    {!> docs_src/getting_started/lifespan/redis/manual_run.py !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="17 21 28-29 31"
    {!> docs_src/getting_started/lifespan/mqtt/manual_run.py !}
    ```

Always call `stop()` in a `finally` block: it acknowledges in-flight messages, closes the connections, and runs your shutdown hooks. A process that just exits after `start()` leaves unacknowledged messages behind.

!!! note
    `start()` does not enter the `#!python FastStream(lifespan=...)` context manager. If you rely on it, enter it yourself around `start()` / `stop()`, or move that logic into `on_startup` / `after_shutdown` hooks.

## `exit()`

`exit()` is how you stop a running `run()` from the inside. It only sets a flag, so it is safe to call from a handler, a hook, or another task:

```python
from faststream import Context, FastStream

@broker.subscriber("control")
async def handle(command: str, app: FastStream = Context()) -> None:
    if command == "shutdown":
        app.exit()
```

`run()` notices the flag on its next tick and proceeds with the normal shutdown sequence. `exit()` has no effect on an application driven by `start()` / `stop()` — there is no loop to leave.

## Several Brokers and Applications

One application can serve any number of brokers: pass them all to the constructor and `run()` starts and stops them together. See [Multiple Brokers](../multiple_brokers.md){.internal-link}.

Running **two applications** in one process is different. Each `run()` installs the same signal handlers, and the loop keeps only the last one, so the application that registered first never receives `exit()` and never shuts down. Either merge the brokers into a single `FastStream`, or drive both applications with `start()` / `stop()` under your own signal handling, as shown above.

## ASGI

When the application is served as an [ASGI app](../asgi.md){.internal-link} through `#!python app.as_asgi()`, the ASGI server owns the process: it handles the signals and reports startup and shutdown through the ASGI lifespan protocol. **FastStream** reacts to those events with `start()` and `stop()`, so the same hooks run, but `run()` and `exit()` are not involved.

## Tests

[TestApp](./test.md){.internal-link} calls `start()` on enter and `stop()` on exit, so lifespan hooks are exercised in tests without signal handling or a blocking loop.
