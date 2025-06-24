---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Exception Middleware

Sometimes, you need to register exception processors at the top level of your application instead of within each message handler.

For this purpose, **FastStream** provides a special `ExceptionMiddleware`. You just need to create it, register handlers, and add it to the broker, router, or subscribers you want (as a [regular middleware](index.md){.internal-link}).

```python linenums="1"
from faststream import ExceptionMiddleware

exception_middleware = ExceptionMiddleware()

Broker(middlewares=[exception_middleware])
```

This middleware can be used in two ways, which we will discuss further.

## General Exceptions Processing

The first way is general exception processing. This is the default case, which can be used to log exceptions correctly, perform cleanup, etc. This type of handler processes all sources of errors such as message handlers, parser/decoder, other middlewares, and publishing. However, it **cannot be used to publish** a default value in response to a request.

You can register such handlers in two ways:

1. By using the middleware's `#!python @add_handler(...)` decorator:
    ```python linenums="1" hl_lines="3"
    exc_middleware = ExceptionMiddleware()

    @exc_middleware.add_handler(Exception)
    def error_handler(exc: Exception) -> None:
        print(repr(exc))
    ```

2. By using the middleware's `handlers` initialization option:
    ```python  linenums="1" hl_lines="5-7"
    def error_handler(exc: Exception) -> None:
        print(repr(exc))

    exc_middleware = ExceptionMiddleware(
        handlers={
            Exception: error_handler
        }
    )
    ```

## Publishing Exceptions Handlers

The second way to process messages is to fallback to a default result that should be published in case of an error. Such handlers can process errors in your message handler (or serialization) function only.

They can be registered in the same two ways as the previous one, but with a slight difference:

1. By using the middleware's `#!python @add_handler(..., publish=True)` decorator:
    ```python linenums="1" hl_lines="3"
    exc_middleware = ExceptionMiddleware()

    @exc_middleware.add_handler(Exception, publish=True)
    def error_handler(exc: Exception) -> str:
        print(repr(exc))
        return "error occurred"
    ```

2. By using the middleware's `publish_handlers` initialization option:
    ```python  linenums="1" hl_lines="6-8"
    def error_handler(exc: Exception) -> str:
        print(repr(exc))
        return "error occurred"

    exc_middleware = ExceptionMiddleware(
        publish_handlers={
            Exception: error_handler
        }
    )
    ```

## Handler Requirements

Your registered exception handlers are also wrapped by the **FastDepends** serialization mechanism, so they can be:

* Either sync or async
* Able to access the [Context](../context/index.md){.internal-link} feature

This works in the same way as a regular message handler.

For example, you can access a consumed message in your handler as follows:

```python linenums="1" hl_lines="8"
from faststream import ExceptionMiddleware, Context

exc_middleware = ExceptionMiddleware()

@exc_middleware.add_handler(Exception, publish=True)
def base_exc_handler(
    exc: Exception,
    message = Context(),
) -> str:
    print(exc, message)
    return "default"
```
