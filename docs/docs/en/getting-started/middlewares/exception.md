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

Sometimes, it is necessary to register exception handlers at the top level of your application, rather than within each individual message handler.

To achieve this, **FastStream** offers a special `ExceptionMiddleware` feature. You simply need to create this middleware, register handlers for it, and add it to the broker, router, or subscribers that you want [like any other middleware](index.md){.internal-link}.

```python linenums="1"
from faststream import ExceptionMiddleware

exception_middleware = ExceptionMiddleware()

broker = Broker(middlewares=[exception_middleware])
```

This middleware can be used in two main ways, which we will discuss in more detail.

## General Exceptions Processing

The first approach is general exception handling. This is the default option, which can be used to correctly log exceptions, perform cleanup, and so on. This type of error handler processes all sources of errors, such as message handlers, parsers/decoders, other middleware, and publishing. However, it cannot be used to return a default value in response to a request.

You can register these handlers in two ways:

### 1. Using the `#!python @add_handler` decorator

```python linenums="1" hl_lines="5-7"
from faststream import ExceptionMiddleware

exc_middleware = ExceptionMiddleware()

@exc_middleware.add_handler(Exception)
async def error_handler(exc: Exception) -> None:
    print(repr(exc))
```

### 2. Using the `handlers` initialization option

```python linenums="1" hl_lines="7"
from faststream import ExceptionMiddleware

async def error_handler(exc: Exception) -> None:
    print(repr(exc))

exc_middleware = ExceptionMiddleware(
    handlers={
        Exception: error_handler
    }
)
```

## Publishing Exceptions Handlers

The second way to handle messages is by using a default result, which should be published if an error occurs. These handlers can only process errors in the message handler or serialization function.

They can be registered in two ways, similar to the previous method, but with a slight difference:

### 1. Using the `!#python @add_handler(..., publish=True)` decorator:

```python linenums="1" hl_lines="5"
from faststream import ExceptionMiddleware

exc_middleware = ExceptionMiddleware()

@exc_middleware.add_handler(Exception, publish=True)
async def error_handler(exc: Exception) -> str:
    print(repr(exc))
    return "error occurred"
```

### 2. Using the `publish_handlers` initialization option

```python linenums="1" hl_lines="8"
from faststream import ExceptionMiddleware

async def error_handler(exc: Exception) -> str:
    print(repr(exc))
    return "error occurred"

exc_middleware = ExceptionMiddleware(
    publish_handlers={
        Exception: error_handler
    }
)
```

## Handler Requirements

Your registered exception handlers are also wrapped by the **FastDepends** serialization mechanism, which allows them to be:

- Sync or async
- Access the [Context](../context/index.md){.internal-link} feature

This works the same as a regular message handler.

For example, you can access the consumed message in your handler by using the following code:

```python linenums="1" hl_lines="10"
from typing import Any
from faststream import ExceptionMiddleware, Context
from faststream.message import StreamMessage

exc_middleware = ExceptionMiddleware()

@exc_middleware.add_handler(Exception, publish=True)
async def base_exc_handler(
    exc: Exception,
    message: StreamMessage[Any] = Context(),
) -> str:
    print(exc, message)
    return "default"
```

## Complete Example

Here is a detailed example demonstrating how to use both types of error handling mechanisms:

```python linenums="1" hl_lines="9 16 29 42"
from typing import Any
from faststream import FastStream, ExceptionMiddleware, Context, Broker
from faststream.message import StreamMessage

# Create exception middleware
exception_middleware = ExceptionMiddleware()

# General exception handler for logging
@exception_middleware.add_handler(ValueError)
async def handle_value_error(exc: ValueError) -> None:
    print(f"ValueError occurred: {exc}")
    # Perform cleanup or logging
    # This handler cannot return a value to publish

# Publishing exception handler for connection errors
@exception_middleware.add_handler(ConnectionError, publish=True)
async def handle_connection_error(
    exc: ConnectionError,
    message: StreamMessage[Any] = Context(),
) -> dict[str, Any]:
    print(f"Connection failed: {exc}")
    return {
        "error": "service_unavailable",
        "message": "External service is temporarily unavailable",
        "retry_after": 300
    }

# Publishing exception handler for validation errors
@exception_middleware.add_handler(TypeError, publish=True)
async def handle_type_error(
    exc: TypeError,
    message: StreamMessage[Any] = Context(),
) -> dict[str, str]:
    print(f"Type error: {exc}")
    return {
        "error": "invalid_data",
        "message": "The provided data format is invalid"
    }

# General exception handler for all exceptions
@exception_middleware.add_handler(Exception)
async def handle_general_error(
    exc: Exception,
    message: StreamMessage[Any] = Context(),
) -> None:
    print(f"Unhandled exception: {exc}")
    print(f"Message: {message}")
    # Log to external service, send alerts, etc.


# Apply middleware to broker
broker = Broker(middlewares=[exception_middleware])

@broker.subscriber("input_topic")
@broker.publisher("output_topic")
async def process_message(msg: dict[str, Any]) -> dict[str, Any]:
    if msg.get("action") == "raise_value_error":
        raise ValueError("This is a test ValueError")

    if msg.get("action") == "raise_connection_error":
        raise ConnectionError("Database connection failed")

    if msg.get("action") == "raise_type_error":
        raise TypeError("Invalid data type provided")

    return {"status": "processed", "data": msg}
```

## Handler Priority

Exception handlers are processed in the order in which they were registered. The first handler that matches the type of exception (including inheritance) will be executed.

```python linenums="1" hl_lines="11"
from faststream import ExceptionMiddleware

exc_middleware = ExceptionMiddleware()

# More specific handler should be registered first
@exc_middleware.add_handler(ValueError, publish=True)
async def handle_value_error(exc: ValueError) -> str:
    return "value_error_handled"

# General handler will catch all other exceptions
@exc_middleware.add_handler(Exception, publish=True)
async def handle_general_error(exc: Exception) -> str:
    return "general_error_handled"
```

## Error Handling Best Practices

1. **Use specific exception types** when possible instead of catching all exceptions
2. **Log errors appropriately** - use general handlers for logging, publishing handlers for responses
3. **Provide meaningful error responses** when using publishing handlers
4. **Consider the order of handler registration** - more specific handlers should be registered first
5. **Use context access** to get additional information about the failed message
6. **Handle both sync and async handlers** - the middleware supports both

This exception middleware provides a robust way to handle errors across your FastStream application. It ensures that exceptions are properly logged and handled, and it can provide meaningful responses to message consumers.
