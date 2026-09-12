---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# RabbitMQ Connection

`RabbitBroker` accepts the connection settings either as a single **AMQP URL** or as separate keyword arguments. You can mix both: explicit keyword arguments override the matching part of the URL.

## Connection URL

The first positional argument is the URL. It defaults to `#!python "amqp://guest:guest@localhost:5672/"`, so a broker created without arguments connects to a local RabbitMQ with the default credentials.

```python linenums="1" hl_lines="3"
{! docs_src/rabbit/connection/url.py !}
```

The URL carries the login, the password, the host, the port and the virtual host. Query parameters are passed to **aio-pika** as-is, so `#!python "amqp://localhost/?heartbeat=30"` sets the heartbeat interval.

An `amqps://` scheme enables TLS and switches the default port to `5671`. See [Security Configuration](./security.md){.internal-link} for the certificate setup.

## Keyword Arguments

If the settings come from different places (environment variables, a secrets manager, a config object), pass them separately instead:

```python linenums="1" hl_lines="5-8"
{! docs_src/rabbit/connection/params.py !}
```

| Argument      | Overrides in URL | Notes                                                         |
| ------------- | ---------------- | ------------------------------------------------------------- |
| `host`        | host             |                                                               |
| `port`        | port             | Defaults to `5672`, or `5671` when TLS is enabled.            |
| `virtualhost` | path             | See below.                                                    |
| `security`    | user, password   | `SASLPlaintext` carries the credentials, `BaseSecurity` TLS.  |
| `ssl_options` | -                | Extra TLS options passed to **aio-pika**.                     |

Credentials are not separate arguments: put them into the URL or use a [security object](./security.md){.internal-link}.

## Virtual Host

The virtual host is the path part of the URL. `#!python "amqp://localhost:5672/my_vhost"` connects to `my_vhost`, and a trailing slash (or no path at all) means the default `/` virtual host.

The `virtualhost` argument sets the same thing and wins over the URL path, so `#!python RabbitBroker("amqp://localhost/from_url", virtualhost="from_arg")` connects to `from_arg`.

!!! warning "One connection, one virtual host"
    An AMQP connection belongs to exactly one virtual host, and a `RabbitBroker` owns exactly one connection. To work with several virtual hosts, create a broker per virtual host and pass all of them to the application:

    ```python linenums="1" hl_lines="4-5 7"
    {! docs_src/rabbit/connection/multiple_vhosts.py !}
    ```

    Each broker keeps its own subscribers and publishers. The [Multiple Brokers](../getting-started/multiple_brokers.md){.internal-link} page covers how such an application starts, stops and is tested.

## `connect()` and `start()`

`RabbitBroker` has two entry points that are easy to confuse:

* `#!python await broker.connect()` only opens the AMQP connection and a default channel. Nothing is declared and no subscriber is consuming. This is what you need when the broker acts as a **client**: publishing messages, sending [RPC requests](./rpc.md){.internal-link} or [declaring queues manually](./declare.md){.internal-link}. Using the broker as an async context manager (`#!python async with broker:`) calls `connect()` on enter and `stop()` on exit.
* `#!python await broker.start()` calls `connect()` first, then declares every exchange and queue that your subscribers and publishers refer to, and finally starts consuming. This is what `FastStream` does for you on application startup, so you should not call it yourself inside an application.

!!! tip
    `connect()` is idempotent: calling it after the broker was already started does nothing. That's why a `#!python broker.publish(...)` inside a handler never opens a second connection.
