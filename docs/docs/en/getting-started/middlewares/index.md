---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Middlewares

**Middlewares** are a powerful mechanism that allows you to add additional logic to any stage of the message processing pipeline.

This way, you can greatly extend your **FastStream** application with features such as:

* Integration with any logging/metrics systems
* Application-level message serialization logic
* Rich publishing of messages with extra information
* And many other capabilities

**Middlewares** have several methods to override. You can implement some or all of them and use middlewares at the broker, router, or subscriber level. Thus, middlewares are the most flexible  **FastStream** feature.

## Message Processing Middleware

Unfortunately, this powerful feature has a somewhat complex signature too.

Using middlewares, you can wrap the entire message processing pipeline. In this case, you need to specify `on_receive` and `after_processed` methods:

```python linenums="1"
from faststream import BaseMiddleware

class MyMiddleware(BaseMiddleware):
    async def on_receive(self):
        print(f"Received: {self.msg}")
        return await super().on_receive()

    async def after_processed(self, exc_type, exc_val, exc_tb):
        return await super().after_processed(exc_type, exc_val, exc_tb)
```

These methods should be overridden only in a broker-level middlewares.

```python
Broker(middlewares=[MyMiddleware])
```

Also, you can use `BaseMiddleware` inheritors as [router](../routers/index.md)-level dependencies as well(they will be applied only to objects created by this router):

```python
BrokerRouter(middlewares=[MyMiddleware])
```

!!! tip
    Please always call `#!python super()` methods at the end of your function; this is important for correct error processing.

## Subscriber Middleware

Subscriber middlewares will be called at your `handler` function call. Using it, you can patch the incoming message body right before passing it to your consumer subscriber or catch any handler exception by returning a fallback value to publish (the middleware return value will be published then).

In this case, you need to implement the `consume_scope` middleware method:

```python linenums="1"
from typing import Callable, Awaitable

from faststream import BaseMiddleware
from faststream.broker.message import StreamMessage

class MyMiddleware(BaseMiddleware):
    async def consume_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        return await call_next(msg)


Broker(middlewares=[MyMiddleware])
```

!!! note
    The `msg` option always has the already decoded body. To prevent the default `#!python json.loads(...)` call, you should use a [custom decoder](../serialization/decoder.md) instead.

If you want to apply such middleware to a specific subscriber instead of the whole application, you can just create a function with the same signature and pass it right to your subscriber:

```python linenums="1" hl_lines="10"
async def subscriber_middleware(
    call_next: Callable[[Any], Awaitable[Any]],
    msg: StreamMessage[Any],
) -> Any:
    return await call_next(msg)


@broker.subscriber(
    ...,
    middlewares=[subscriber_middleware],
)
async def handler():
    ...
```

## Publisher Middlewares

Finally, using middlewares, you are able to patch outgoing messages too. For example, you can compress/encode outgoing messages at the application level or add custom types serialization logic.

Publisher middlewares can be applied at the **broker**, **router** or each **publisher** level. **Broker** publisher middlewares affect all the ways to publish something (including `#!python broker.publish` call).

In this case, you need to specify the `publish_scope` method:

```python linenums="1"
from typing import Callable, Awaitable

from faststream import BaseMiddleware

class MyMiddleware(BaseMiddleware):
    async def publish_scope(
        self,
        call_next: Callable[..., Awaitable[Any]],
        msg: Any,
        **options: Any,
    ) -> Any:
        return await call_next(msg, **options)


Broker(middlewares=[MyMiddleware])
```

This method consumes the message body to send and any other options passing to the `publish` call (destination. headers, etc).

Also, you can specify middleware for publisher object as well. In this case, you should create a function with the same `publish_scope` signature and use it as a publisher middleware:

```python linenums="1" hl_lines="12"
async def publisher_middleware(
    call_next: Callable[..., Awaitable[Any]],
    msg: Any,
    **options: Any,
) -> Any:
    return await call_next(msg, **options)


@broker.subscriber(...)
@broker.publisher(
    ...,
    middlewares=[publisher_middleware],
)
async def handler():
    ...
```

!!! note
    If you are using `publish_batch` somewhere in your app, your publisher middleware should consume `#!python *msgs` option additionally.
