---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# ASGI Support

Often, you need not only to run your application to consume messages but also to make it a part of your service ecosystem with *Prometheus metrics*, K8S *liveness* and *readiness probes*, *traces*, and other observability features.

Unfortunately, such functionality can't be implemented by broker features alone, and you have to provide several **HTTP** endpoints in your app.

Of course, you can use **FastStream** as a part of any **ASGI** frameworks ([integrations](./integrations/frameworks/index.md){.internal-link}), but fewer the dependencies, the better, right?

## AsgiFastStream

Fortunately, we have built-in **ASGI** support. It is very limited but good enough to provide you with basic functionality for metrics and healthcheck endpoint implementation.

Let's take a look at the following example:

```python linenums="1" hl_lines="2 5" title="main.py"
from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream

broker = NatsBroker()
app = AsgiFastStream(broker)
```

This simple example allows you to run the app using regular **ASGI** servers:

```shell
uvicorn main:app
```

It does nothing but launch the app itself as an **ASGI lifespan**.

!!! note
    If you want to run your app using several workers, you need to use something else than `uvicorn`.
    ```shell
    faststream run main:app --workers 4
    ```
    ```shell
    gunicorn -k uvicorn.workers.UvicornWorker main:app --workers=4
    ```
    ```shell
    granian --interface asgi main:app --workers 4
    ```
    ```shell
    hypercorn main:app --workers 4
    ```


### ASGI Routes

It doesn't look very helpful, so let's add some **HTTP** endpoints.

First, we have already written a wrapper on top of the broker to make a ready-to-use **ASGI** healthcheck endpoint for you:

```python linenums="1" hl_lines="2 9"
from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream, make_ping_asgi

broker = NatsBroker()

app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/health", make_ping_asgi(broker, timeout=5.0)),
    ]
)
```

!!! note
    This `/health` endpoint calls the `#!python broker.ping()` method and returns **HTTP 204** or **HTTP 500** statuses.

### Custom ASGI Routes

**AsgiFastStream** is able to call any **ASGI**-compatible callable objects, so you can use any endpoints from other libraries if they are compatible with the protocol.

If you want to write your own simple **HTTP**-endpoint, you can use our `#!python @get` decorator as in the following example:

```python linenums="1" hl_lines="2 6-8 12"
from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream, AsgiResponse, get

broker = NatsBroker()

@get
async def liveness_ping(scope):
    return AsgiResponse(b"", status_code=200)

app = AsgiFastStream(
    broker,
    asgi_routes=[("/health", liveness_ping)]
)
```

!!! tip
    You do not need to setup all routes using the `asgi_routes=[]` parameter.<br/>
    You can use the `#!python app.mount("/health", asgi_endpoint)` method also.

### AsyncAPI Documentation

You can also host your **AsyncAPI** documentation in the same process, by running [`#!shell faststream docs serve ...`](./asyncapi/hosting.md){.internal-link}, in the same container and runtime.

Just create an `AsgiFastStream` object with a special option:

```python linenums="1" hl_lines="8"
from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream

broker = NatsBroker()

app = AsgiFastStream(
    broker,
    asyncapi_path="/docs",
)
```

Now, your **AsyncAPI HTML** representation can be found by the `/docs` url.

### FastStream Object Reuse

You may also use regular `FastStream` application object for similar result.

```python linenums="1" hl_lines="2 11"
from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.asgi import make_ping_asgi, AsgiResponse

broker = NatsBroker()

@get
async def liveness_ping(scope):
    return AsgiResponse(b"", status_code=200)

app = FastStream(broker).as_asgi(
    asgi_routes=[
        ("/liveness", liveness_ping),
        ("/readiness", make_ping_asgi(broker, timeout=5.0)),
    ],
    asyncapi_path="/docs",
)
```

!!! tip
    For apps that use ASGI, you may use the CLI command just like for the default FastStream app

    ```shell
    faststream run main:app --host 0.0.0.0 --port 8000 --workers 4
    ```
    This possibility built on gunicorn + uvicorn, you need install them to run FastStream ASGI app via CLI.
    We send all args directly to gunicorn, you can learn more about it [here](https://github.com/benoitc/gunicorn/blob/master/examples/example_config.py).

## Other ASGI Compatibility

Moreover, our wrappers can be used as ready-to-use endpoins for other **ASGI** frameworks. This can be very helpful When you are running **FastStream** in the same runtime as any other **ASGI** frameworks.

Just follow the following example in such cases:

```python linenums="1" hl_lines="6 19-20"
from contextlib import asynccontextmanager

from fastapi import FastAPI
from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.asgi import make_ping_asgi, make_asyncapi_asgi

broker = NatsBroker()

@asynccontextmanager
async def start_broker(app):
    """Start the broker with the app."""
    async with broker:
        await broker.start()
        yield

app = FastAPI(lifespan=start_broker)

app.mount("/health", make_ping_asgi(broker, timeout=5.0))
app.mount("/asyncapi", make_asyncapi_asgi(FastStream(broker)))
```

!!! tip
    You can also bind to unix domain or a file descriptor. FastStream will bind to “127.0.0.1:8000” by default

    ```shell
    faststream run main:app --bind unix:/tmp/socket.sock
    ```
    ```shell
    faststream run main:app --bind fd://2
    ```
    You can use multiple binds if you want
    ```shell
    faststream run main:app --bind 0.0.0.0:8000 '[::]:8000'
    ```
