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

Most of the time you start an application with `faststream run module:app` and never call any of the methods below. You need them when:

* you want to start the application from your own script instead of the CLI,
* **FastStream** has to live inside a process that already has its own `main` (another framework, a scheduler, several applications at once),
* the application must stop itself from a handler or a hook.

| Method    | What it does                                                                                   |
| --------- | ---------------------------------------------------------------------------------------------- |
| `run()`   | Starts the application and blocks until the process is told to stop. What the CLI calls.        |
| `start()` | Runs the startup hooks and starts every broker. Returns as soon as they are consuming.        |
| `stop()`  | Waits for in-flight handlers, runs the shutdown hooks and stops every broker.                  |
| `exit()`  | Asks a running `run()` to stop.                                                               |

## Running Without the CLI

`faststream run module:app` does nothing more than import the module and await `#!python app.run()`, so you can do the same yourself:

```python
import asyncio

asyncio.run(app.run())
```

`run()` handles <kbd>Ctrl</kbd>+<kbd>C</kbd> and `SIGTERM` for you: either one shuts the application down cleanly instead of leaving a traceback. It also enters the `#!python FastStream(lifespan=...)` context manager, if you passed one (see [Lifespan Option](./context.md){.internal-link}).

Because `run()` blocks until the process is told to stop, it should be the only thing your `main` awaits. Anything else you need to run alongside belongs in a [lifespan hook](./hooks.md){.internal-link} or in the `lifespan` context manager.

## Embedding Into Your Own Event Loop

When something else already owns the process, use `start()` and `stop()` instead of `run()`. They run the same [lifespan hooks](./hooks.md){.internal-link}, so `on_startup` / `after_startup` and `on_shutdown` / `after_shutdown` behave exactly as under `run()`. What they don't do is take over the process: no signal handlers are installed, and your code decides when to stop.

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

Always call `stop()` in a `finally` block. It waits for the handlers that are still processing a message (up to the broker's `graceful_timeout`), runs your shutdown hooks and closes the connections. If the process exits without it, messages that were being handled are never acknowledged, so the broker delivers them to the next consumer and they get processed twice.

!!! note
    `start()` does not enter the `#!python FastStream(lifespan=...)` context manager. If you rely on it, enter it yourself around `start()` / `stop()`, or move that logic into `on_startup` / `after_shutdown` hooks.

## Stopping From Inside the Application

`exit()` stops a running `run()`. It is safe to call from a handler, a hook, or any other task:

```python
from faststream import Context, FastStream

@broker.subscriber("control")
async def handle(command: str, app: FastStream = Context()) -> None:
    if command == "shutdown":
        app.exit()
```

The application then goes through the same shutdown as on <kbd>Ctrl</kbd>+<kbd>C</kbd>. `exit()` has no effect on an application driven by `start()` / `stop()`: there is no `run()` to stop, so use your own stop signal, as the example above does.

## Several Brokers and Applications

One application can serve any number of brokers: pass them all to the constructor and `run()` starts and stops them together. See [Multiple Brokers](../multiple_brokers.md){.internal-link}.

Don't run **two applications** with `run()` in one process. Each `run()` installs its own signal handlers, and the second one replaces the first, so the first application never gets the signal and never shuts down. Either merge the brokers into a single `FastStream`, or drive both applications with `start()` / `stop()` under your own signal handling, as shown above.

## ASGI

When the application is served as an [ASGI app](../asgi.md){.internal-link} through `#!python app.as_asgi()`, the ASGI server owns the process and handles the signals. **FastStream** starts and stops together with the server, so the same hooks run, but `run()` and `exit()` are not involved.

## Tests

[TestApp](./test.md){.internal-link} calls `start()` on enter and `stop()` on exit, so lifespan hooks run in tests without signal handling or a blocking loop.
