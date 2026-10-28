---
description: >-
  Observe NATS connection errors, disconnections, closure, server discovery, and
  reconnection with asynchronous callbacks on FastStream's NatsBroker.
search:
  boost: 10
---

# NATS Broker Events

Use broker event callbacks to observe the NATS client's connection: log failures, record metrics, or signal a change in availability to other parts of your application. These callbacks apply to the connection used by both Core NATS and JetStream.

Pass callbacks as keyword arguments when creating `NatsBroker`. FastStream uses the corresponding callbacks from **nats-py**. Their signatures are documented in the official [nats-py connection API](https://nats-io.github.io/nats.py/modules.html#nats.aio.client.Client.connect){.external-link target="_blank"}.

## Register callbacks

Each callback must be an async function returning `None`. Only `error_cb` receives an argument: the reported `Exception`. The other four callbacks take no arguments. All five arguments are optional and default to `None`. Register only the events your application needs.

!!! note "Built-in connection logging"
    FastStream wraps `error_cb` and `reconnected_cb` with its built-in connection logging. Depending on the broker's connection state, these wrappers can log `Connection broken with ...` at `WARNING` and `Connection established` at `INFO`. If you provide a callback, FastStream awaits it before running the wrapper's own logic. The wrappers are also used when these callbacks are `None`.

```python linenums="1" hl_lines="10-27 32-36"
{!> docs_src/nats/broker_events.py !}
```

With a NATS server running locally, save this example as `broker_events.py` and run:

```shell
faststream run broker_events:app
```

To observe disconnection and reconnection, keep the application running, stop the NATS server, and restart it before the retry limit is exhausted. Stop the application with `Ctrl+C` to observe disconnection and closure during normal shutdown. Server discovery requires a cluster advertising new servers, so it will not occur with a standalone server.

!!! note "Callback dependencies"
    The NATS client awaits these callbacks directly. FastStream dependency injection does not apply to them: do not add `Logger`, `Context`, or message parameters to a connection callback. The example uses a standard Python logger for callbacks and an injected `Logger` for the subscriber.

## Callback reference

| Argument               | Signature                                      | Event                                                                             |
|------------------------|------------------------------------------------|-----------------------------------------------------------------------------------|
| `error_cb`             | `async def on_error(error: Exception) -> None` | The client reports an error.                                                      |
| `disconnected_cb`      | `async def on_disconnected() -> None`          | The client disconnects from NATS, including during normal shutdown.               |
| `closed_cb`            | `async def on_closed() -> None`                | The NATS connection is closed. The client will no longer reconnect automatically. |
| `discovered_server_cb` | `async def on_discovered_server() -> None`     | The client learns about new servers through a cluster update.                     |
| `reconnected_cb`       | `async def on_reconnected() -> None`           | The client successfully reconnects after a disconnection.                         |

### Errors: `error_cb`

Use this callback to report errors from the NATS client, such as a failed connection or reconnection attempt, a server-reported permission violation, or a slow consumer whose pending message queue is full. The exception helps you distinguish transport problems from other client errors. An error notification alone does not mean that the connection has closed. The callback can also run during the initial broker connection, before application startup completes.

This callback is not a catch-all exception handler for your application. Operations such as publishing or making a request can raise directly to their caller. Handle those exceptions around the operation. Use [`ExceptionMiddleware`](../getting-started/middlewares/exception.md){.internal-link} for FastStream message-processing error handling.

### Disconnection: `disconnected_cb`

Use this callback to record that the connection is unavailable, for example after a server restart or a network failure. If automatic reconnection is enabled, the client attempts to restore the connection according to its retry settings. You do not need to call `broker.connect()` inside the callback.

The client also invokes this callback when closing the connection deliberately. If you track availability or raise alerts, account for normal application shutdown.

### Closure: `closed_cb`

Use this callback to observe the end of the connection's lifecycle. It runs when the client closes the connection, including normal FastStream shutdown, or when it gives up reconnecting. For example, this can happen after the retry limit is exhausted or when reconnection is disabled.

A disconnection can be temporary. Closure means this client connection will not recover automatically. The callback does not itself stop the FastStream application. Keep application shutdown decisions in your application's lifecycle code.

### Server discovery: `discovered_server_cb`

In a cluster, a NATS server can advertise other servers through `INFO` updates. The client adds previously unknown addresses to its server pool and invokes this callback when such an update adds servers after the initial connection handshake. Use it to log changes in the available reconnection targets.

The callback receives no server address and can represent discovery of several servers in one update. It does not mean the client connected to those servers, and it is not a notification for server removal. Servers learned during the initial handshake do not trigger this callback.

Once the broker is connected, use `broker.connection.discovered_servers` to read the discovered server addresses. This property returns the discovered servers in the current pool, rather than only the addresses from the latest update.

### Reconnection: `reconnected_cb`

Use this callback to record recovery after an interruption. The client invokes it after it has re-established the connection and restored its subscriptions. It can reconnect to the same server or another server in its pool. Do not register your subscribers again inside the callback. Use `broker.connection.connected_url` to read the server address after reconnection.

This callback does not run on the first successful connection. For application initialization after the broker starts, use FastStream's [`after_startup` hook](../getting-started/lifespan/hooks.md){.internal-link}.

## Reconnection and application behavior

`NatsBroker` enables automatic reconnection by default through `allow_reconnect=True`. `reconnect_time_wait` controls the delay between attempts, and `max_reconnect_attempts` limits retries per server. Callbacks observe that process. Registering them does not enable or disable retries. See the official [NATS reconnection guide](https://docs.nats.io/learn/resilient-clients/reconnection){.external-link target="_blank"} for the underlying behavior.

A typical recovery produces `disconnected_cb`, possibly `error_cb` notifications for failed attempts, and then `reconnected_cb`. If recovery fails and the client closes, `closed_cb` runs instead. During deliberate shutdown, both `disconnected_cb` and `closed_cb` can run without any later reconnection.

Keep callbacks short: the client awaits them during connection processing. Use them to update a flag or signal an `asyncio.Event`, and let a separate task perform slow work. Avoid blocking I/O, letting exceptions escape, or awaiting broker shutdown or connection-dependent operations inside a callback.

!!! note "Reconnection and message delivery"
    A successful reconnection does not recover Core NATS messages published by other clients while your subscriber was disconnected. Use [JetStream](./jetstream/index.md){.internal-link} when messages need persistence. The official [NATS connection lifecycle guide](https://docs.nats.io/learn/core-nats/connection-lifecycle){.external-link target="_blank"} explains the distinction between connection recovery and message delivery.
