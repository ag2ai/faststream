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

**Middlewares** are a powerful tool that allows you to add extra logic to any part of the message processing pipeline.

This way, you can significantly enhance your **FastStream** application with features such as:

- Integration with various logging and metrics systems
- Application-level message serialization logic
- Extensive publication of messages with additional information
- And numerous other capabilities

**Middlewares** have several methods that can be overridden. You can choose to implement some or all of these methods and use middlewares at different levels, such as the broker and router. This makes middlewares the most flexible feature of FastStream.

## Message Processing Middleware

Unfortunately, this powerful feature also has a somewhat complex implementation.

Using middlewares, you can encapsulate the entire message processing pipeline. In this scenario, you would need to define `on_receive` and `after_process` methods:

```python linenums="1"
from faststream import BaseMiddleware

class MyMiddleware(BaseMiddleware):
    async def on_receive(self):
        print(f"Received: {self.msg}")
        return await super().on_receive()

    async def after_processed(self, exc_type, exc_val, exc_tb):
        return await super().after_processed(exc_type, exc_val, exc_tb)
```

These methods should only be overridden in broker-level middleware.

```python
broker = Broker(middlewares=[MyMiddleware])
```

Also, you can use `BaseMiddleware` inheritors as [Router](../routers/index.md){.internal-link}-level dependencies as well(they will be applied only to objects created by that router):

!!! tip

1. Please always call the `#!python super()` method at the end of your function. This is important for proper error handling.
2. The `msg` option always has the already decoded body. To prevent the default `#!python json.loads(...)` call, you should use a [custom decoder](../serialization/decoder.md){.internal-link} instead.

## Publisher Middlewares

Finally, using middleware, you can also patch outgoing messages. For example, you can compress or encode outgoing messages at the application level, or add custom serialization logic for specific types.

Publisher middlewares can be applied at the **broker**, **router** or each **publisher** level. **Broker** publisher middlewares affect all the ways to publish something (including the `#!python broker.publish` call).

In this case, you need to specify the `publish_scope` method:

```python linenums="1"
from typing import Any, Awaitable, Callable

from faststream import BaseMiddleware
from faststream.response import PublishCommand


class MyMiddleware(BaseMiddleware):
    async def publish_scope(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any:
        return await super().publish_scope(call_next, cmd)


broker = Broker(middlewares=[MyMiddleware])
```

This method consumes the message body and any other options passed to the `publish` function (such as destination headers, etc.).

!!! note
If you are using `publish_batch` somewhere in your app, your publisher middleware should consume `#!python *msgs` option additionally.

## Context Access

Middlewares can access the [Context](../context/index.md){.internal-link} system:

```python linenums="1" hl_lines="14"
from typing import Any, Awaitable, Callable

from faststream import BaseMiddleware
from faststream.message import StreamMessage


class ContextMiddleware(BaseMiddleware):
    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        # Access context
        message_context = self.context.get_local("message")

        # Your middleware logic here
        return await call_next(msg)
```

This approach provides a clean and flexible way to integrate cross-cutting features into FastStream applications, ensuring a clear separation of concerns.

## Full examples

### Retry Middleware

```python
import asyncio
from typing import Any, Awaitable, Callable, Final

from typing_extensions import override

from faststream import BaseMiddleware
from faststream.message import StreamMessage


class RetryMiddleware(BaseMiddleware):
    MAX_RETRIES: Final[int] = 3

    @override
    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                return await call_next(msg)
            except Exception as e:
                if attempt == self.MAX_RETRIES:
                    print(f"Failed after {self.MAX_RETRIES} retries: {e}")
                    raise

                print(f"Attempt {attempt + 1} failed, retrying: {e}")
                await asyncio.sleep(2**attempt)  # Exponential backoff
        return None


broker = Broker(middlewares=[RetryMiddleware])
# Or: router = BrokerRouter(middlewares=[RetryMiddleware])
```

## Summary

**Middlewares** are a powerful tool that allows you to add custom logic to the FastStream message processing pipeline.

### Key Capabilities:

- Integration with logging and metrics systems
- Message serialization logic at the application level
- Improved message publishing with additional data

### Types of Middlewares:

1. **Message Processing Middleware**

   - Methods: `!#python on_receive()` and `!#python after_processed()`
   - **Only** applied at broker level: `!#python Broker(middlewares=[MyMiddleware])`
   - These methods should only be overridden in broker-level middleware

2. **Publisher/Subscriber Middleware**

   - Methods: `#!python publish_scope()` and `#!python consume_scope()`
   - Applied at broker, router

3. **Context Access**
   - Access via `#!python self.context`
