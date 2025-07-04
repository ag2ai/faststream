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

**Middlewares** are a powerful tool that allows you to add extra logic to any stage of the message processing pipeline. This way, you can extend your **FastStream** application with various features:

- Integration with logging/metrics systems
- Custom message serialization logic at the application level
- Publishing of messages with additional information
- Validation and transformation of requests and responses
- Error handling and retries
- And many more capabilities

**Middlewares** have several methods that you can override. You can choose to implement some or all of these methods and use middlewares at different stages of the pipeline, such as at the broker or router level. Middlewares are one of the most flexible features of **FastStream**.

## Types of Middlewares

**FastStream** supports three main types of middleware, each with a different purpose:

### 1. Message Processing Middlewares

These middlewares wrap the entire message processing lifecycle and are only applied at the broker level.

### 2. Consumer Middlewares

These middleware intercept messages before they reach your handler function and can be applied at the broker, router, or subscriber levels.

### 3. Publisher Middlewares

These middlewares intercept outgoing messages and can be applied at the **broker** or **router**.

## Creating Custom Middlewares

### Basic Middleware Structure

All middlewares inherit from `BaseMiddleware`:

```python linenums="1" hl_lines="1"
from faststream import BaseMiddleware

class MyMiddleware(BaseMiddleware):
    # Override methods as needed
    pass
```

### Message Processing Middlewares (Broker Level)

These middleware components control the entire message lifecycle using the `on_receive` and `after_processed` methods.

```python linenums="1" hl_lines="5 11 24"
from types import TracebackType
from faststream import BaseMiddleware, Broker

class MessageProcessingMiddleware(BaseMiddleware):
    async def on_receive(self) -> None:
        """Called when a message is received, before any processing"""
        print(f"Received message: {self.msg}")
        # Always call super() to continue processing
        return await super().on_receive()

    async def after_processed(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        """Called after message processing is complete"""
        if exc_type:
            print(f"Processing failed: {exc_type.__name__}")
        else:
            print("Processing completed successfully")

        # Always call super() at the end
        return await super().after_processed(exc_type, exc_val, exc_tb)

# Apply only to broker
broker = Broker(middlewares=[MessageProcessingMiddleware])
```

!!! tip
Please always call `#!python super()` methods at the end of your function. This is important for proper error handling.

### Consumer Middlewares

Consumer middlewares intercept messages before your handler using the `consume_scope` method:

```python linenums="1" hl_lines="6 20"
from typing import Any, Callable, Awaitable
from faststream import BaseMiddleware
from faststream.message import StreamMessage

class ConsumerMiddleware(BaseMiddleware):
    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        """Intercept message before handler execution"""
        print(f"Processing message: {msg.body}")

        # Modify message if needed
        # msg.body = transform_message(msg.body)

        try:
            # Call the next middleware or handler
            result = await call_next(msg)
            print("Handler completed successfully")
            return result
        except Exception as e:
            print(f"Handler failed: {e}")
            # You can return a fallback value here
            # return "fallback_response"
            raise

# Can be applied at different levels
broker = Broker(middlewares=[ConsumerMiddleware])
# Or
router = BrokerRouter(middlewares=[ConsumerMiddleware])
```

!!! note
The `msg` option always contains the already decoded body. To avoid the default `!#python json.loads(...)` call, you should use a [custom decoder](../serialization/decoder.md){.internal-link} instead.

### Publisher Middlewares

The publisher middleware intercepts outgoing messages using the `publish_scope` method:

```python linenums="1" hl_lines="5"
from typing import Any, Callable, Awaitable
from faststream import BaseMiddleware

class PublisherMiddleware(BaseMiddleware):
    async def publish_scope(
        self,
        call_next: Callable[..., Awaitable[Any]],
        msg: Any,
        **options: Any,
    ) -> Any:
        """Intercept outgoing messages"""
        print(f"Publishing message: {msg}")

        # Modify message or options
        # msg = compress_message(msg)
        # options['headers'] = add_custom_headers(options.get('headers', {}))

        return await call_next(msg, **options)

# Can be applied at different levels
broker = Broker(middlewares=[PublisherMiddleware])
```

This method consumes the message body and any other options passed to the `publish` function (destination, headers, etc.) in order to send the message.

!!! note
If you are using the `publish_batch` function in your application, your publishing middleware should also consume the `!#python *msgs` option.

## Exception Middleware

The `ExceptionMiddleware` is responsible for handling exceptions at the application level. For more detailed information about how exceptions are handled, please see the [Exception Handling](exception.md){.internal-link} documentation.

## Practical Examples

### Request Logging Middleware

```python linenums="1" hl_lines="7"
import time
from typing import Any, Callable, Awaitable
from faststream import BaseMiddleware
from faststream.message import StreamMessage

class RequestLoggingMiddleware(BaseMiddleware):
    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        start_time = time.time()

        try:
            result = await call_next(msg)
        except Exception as e:
            duration = time.time() - start_time
            print(f"Request failed after {duration:.2f}s: {e}")
            raise
        else:
            duration = time.time() - start_time
            print(f"Request completed in {duration:.2f}s")
            return result

```

### Message Validation Middleware

```python linenums="1" hl_lines="18"
from typing import Any, Callable, Awaitable, TypeVar
from pydantic import BaseModel, ValidationError
from faststream import BaseMiddleware
from faststream.message import StreamMessage

T = TypeVar('T', bound=BaseModel)

class ValidationMiddleware(BaseMiddleware):
    def __init__(
        self,
        *args: Any,
        schema: type[T],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.schema = schema

    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        try:
            # Validate message against schema
            validated_data = self.schema.model_validate(msg.body)
        except ValidationError as e:
            print(f"Validation failed: {e}")
            return {"error": "invalid_message", "details": str(e)}
        else:
            # Replace message body with validated data
            msg.body = validated_data
            return await call_next(msg)

```

### Retry Middleware

```python linenums="1" hl_lines="16"
import asyncio
from typing import Any, Callable, Awaitable
from faststream import BaseMiddleware
from faststream.message import StreamMessage

class RetryMiddleware(BaseMiddleware):
    def __init__(
        self,
        *args: Any,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.max_retries = max_retries

    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        for attempt in range(self.max_retries + 1):
            try:
                return await call_next(msg)
            except Exception as e:
                if attempt == self.max_retries:
                    print(f"Failed after {self.max_retries} retries: {e}")
                    raise

                print(f"Attempt {attempt + 1} failed, retrying: {e}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
```

## Best Practices

1. **Always call the `super()` methods** in message processing middleware to ensure proper error handling.
2. **Use appropriate middleware types** for your use case:
   - Message processing middleware: For broker-wide lifecycle control.
   - Consumer: For message interception and transformation.
   - Publisher: For outgoing message modification.
3. **Handle exceptions properly** in middlewares to avoid breaking the processing pipeline.
4. **Keep middlewares lightweight** to avoid performance issues.
5. **Use dependency injection** - middlewares support FastStream's context system.
6. **Test middlewares thoroughly** as they affect the entire message flow.

## Context Access

Middlewares can access the [FastStream's context](../context/index.md){.external-link} system:

```python linenums="1" hl_lines="12"
from typing import Any, Callable, Awaitable
from faststream import BaseMiddleware, Context
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

This approach provides a clean and flexible way to add cross-cutting concerns to FastStream applications, while maintaining a clear separation of responsibilities.
