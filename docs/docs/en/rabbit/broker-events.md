---
description: >-
  Observe RabbitMQ connection closure and reconnection with callbacks on
  FastStream's RabbitBroker.
search:
  boost: 10
---

# RabbitMQ Broker Events

Use broker event callbacks to observe the RabbitMQ connection: log failures, record metrics, or signal a change in availability to other parts of your application.

Pass sequences of callbacks as keyword arguments when creating `RabbitBroker`. FastStream registers each `on_close` callback in aio-pika's `close_callbacks` collection and each `on_reconnected` callback in its `reconnect_callbacks` collection. See the official [connection implementation](https://docs.aio-pika.com/_modules/aio_pika/connection.html){.external-link target="_blank"} and [robust connection implementation](https://docs.aio-pika.com/_modules/aio_pika/robust_connection.html){.external-link target="_blank"} for the underlying events.

## Register callbacks

Both arguments accept a sequence of callbacks, such as a list or tuple, and default to `None`. Pass even a single callback inside a sequence. Each callback can be a sync or async function returning `None`, and a sequence can contain both. Use multiple callbacks to keep actions such as logging and metrics separate. The example uses one async function for each event.

Callback execution order is not guaranteed because aio-pika stores callbacks in a set. Register only the events your application needs.

```python linenums="1" hl_lines="12-23 28-29"
{!> docs_src/rabbit/broker_events.py !}
```

With a RabbitMQ server running locally, save this example as `broker_events.py` and run:

```shell
faststream run broker_events:app
```

To observe closure and reconnection, wait for the application to start, stop RabbitMQ, and restart it while the application is still running. Stop the application with `Ctrl+C` to observe closure during normal shutdown.

!!! note "Callback dependencies"
    aio-pika invokes these callbacks directly. FastStream dependency injection does not apply to them: do not add `Logger`, `Context`, or message parameters to a connection callback. The example uses a standard Python logger for callbacks and an injected `Logger` for the subscriber.

## Callback reference

| Argument         | Signature                                                                                          | Event                                                    |
|------------------|----------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `on_close`       | `async def on_close(connection: AbstractConnection \| None, error: BaseException \| None) -> None` | The connection closes, including during normal shutdown. |
| `on_reconnected` | `async def on_reconnected(connection: AbstractRobustConnection \| None) -> None`                   | The robust connection reconnects.                        |

The connection argument can be `None` according to aio-pika's typing contract. Its callback collections hold a weak reference to the sender. See the official [callback types and collections](https://docs.aio-pika.com/_modules/aio_pika/tools.html){.external-link target="_blank"}.

### Closure: `on_close`

Use this callback to record connection loss, for example after a server restart or a network failure. It also runs when FastStream stops the broker deliberately. The second argument is the closing exception, which can be `None`. Its type is `BaseException` because closure can involve cancellation.

A close notification does not mean that automatic reconnection has stopped. aio-pika can recover from temporary connection loss. Check `connection.close_called`, when the connection argument is available, to distinguish an explicit close from an unexpected interruption. A closing exception alone does not identify an unexpected failure.

aio-pika can also invoke this callback if channel restoration fails. Treat it as a connection notification rather than an application exception handler. Handle exceptions from publishing or requests around the operation. Use [`ExceptionMiddleware`](../getting-started/middlewares/exception.md){.internal-link} for FastStream message-processing error handling.

### Reconnection: `on_reconnected`

Use this callback to record recovery after an interruption. During automatic recovery, aio-pika invokes it after restoring the connection and its robust channels. You do not need to call `broker.connect()` or register your subscribers again inside the callback.

This callback does not run on the first successful connection. For application initialization after the broker starts, use FastStream's [`after_startup` hook](../getting-started/lifespan/hooks.md){.internal-link}.

## Reconnection and application behavior

`RabbitBroker` uses aio-pika's robust connection to reconnect automatically. `reconnect_interval` controls the delay between attempts and defaults to `5.0` seconds. Registering callbacks does not change the retry behavior. `fail_fast` defaults to `True` and controls whether the initial connection attempt fails immediately when RabbitMQ is unavailable.

FastStream registers the callbacks after establishing a connection, including each new connection created after a broker restart. These callbacks do not report failures during initial connection establishment. Handle those errors around broker startup or `connect()`.

A typical interruption produces `on_close`, followed by `on_reconnected` once recovery succeeds. During deliberate shutdown, `on_close` runs without a later reconnection. These callbacks observe the connection lifecycle and do not themselves stop the FastStream application.

Keep callbacks short: aio-pika waits for async callbacks during connection processing. Use them to update a flag or signal an `asyncio.Event`, and let a separate task perform slow work. Avoid blocking I/O or awaiting broker shutdown or connection-dependent operations inside a callback.
