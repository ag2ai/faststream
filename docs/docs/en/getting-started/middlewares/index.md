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

- Integration with any logging/metrics systems
- Application-level message serialization logic
- Rich publishing of messages with extra information
- Request/response validation and transformation
- Error handling and retry mechanisms
- And many other capabilities

**Middlewares** have several methods to override. You can implement some or all of them and use middlewares at the broker, router, or subscriber level. Thus, middlewares are the most flexible **FastStream** feature.

## Types of Middlewares

FastStream supports three main types of middlewares, each serving different purposes:

### 1. Message Processing Middlewares

These middlewares wrap the entire message processing lifecycle and are applied at the **broker level only**.

### 2. Consumer Middlewares

These middlewares intercept messages before they reach your handler function and can be applied at **broker**, **router**, or **subscriber** levels.

### 3. Publisher Middlewares

These middlewares intercept outgoing messages and can be applied at **broker**, **router**, or **publisher** levels.

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

These middlewares control the entire message lifecycle using `on_receive` and `after_processed` methods:

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
Please always call `#!python super()` methods at the end of your function; this is important for correct error processing.

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

If you want to apply such middleware to a specific subscriber instead of the whole application, you can just create a function with the same signature and pass it right to your subscriber:

```python linenums="1" hl_lines="10"
from typing import Any, Callable, Awaitable
from faststream.message import StreamMessage

async def subscriber_middleware(
    call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
    msg: StreamMessage[Any],
) -> Any:
    return await call_next(msg)

@broker.subscriber("topic", middlewares=[subscriber_middleware])
async def handler(msg: Any) -> str:
    return "processed"
```

!!! note
The `msg` option always has the already decoded body. To prevent the default `!#python json.loads(...)` call, you should use a [custom decoder](../serialization/decoder.md){.internal-link} instead.

### Publisher Middlewares

Publisher middlewares intercept outgoing messages using the `publish_scope` method:

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

This method consumes the message body to send and any other options passing to the `publish` call (destination, headers, etc).

Also, you can specify middleware for publisher object as well. In this case, you should create a function with the same `publish_scope` signature and use it as a publisher middleware:

```python linenums="1" hl_lines="3 5"
from typing import Any, Callable, Awaitable

async def publisher_middleware(
    call_next: Callable[..., Awaitable[Any]],
    msg: Any,
    **options: Any,
) -> Any:
    return await call_next(msg, **options)

@broker.subscriber("input_topic")
@broker.publisher("output_topic", middlewares=[publisher_middleware])
async def handler(msg: Any) -> str:
    return f"processed: {msg}"
```

!!! note
If you are using `publish_batch` somewhere in your app, your publisher middleware should consume `!#python *msgs` option additionally.

## Built-in Middlewares

### Acknowledgement Middleware

Controls message [acknowledgment](../acknowledgement.md){.internal-link} behavior:

```python
from faststream.middlewares import AcknowledgementMiddleware, AckPolicy

# The middleware is typically configured at the broker level
# and uses different acknowledgment policies:
# - AckPolicy.ACK_FIRST: Acknowledge immediately on receive
# - AckPolicy.ACK: Acknowledge after successful processing
# - AckPolicy.REJECT_ON_ERROR: Reject message on error
# - AckPolicy.NACK_ON_ERROR: Negative acknowledge on error
# - AckPolicy.DO_NOTHING: Manual acknowledgment control
```

### Exception Middleware

The `ExceptionMiddleware` handles exceptions at the application level. For detailed information about exception handling, see the [Exception Middleware](exception.md){.internal-link} documentation.

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

1. **Always call `super()` methods** in message processing middlewares to ensure proper error handling
2. **Use appropriate middleware types** for your use case:
   - Message processing: For broker-wide lifecycle control
   - Consumer: For message interception and transformation
   - Publisher: For outgoing message modification
3. **Handle exceptions properly** in middlewares to avoid breaking the processing pipeline
4. **Keep middlewares lightweight** to avoid performance impact
5. **Use dependency injection** - middlewares support FastStream's context system
6. **Test middlewares thoroughly** as they affect the entire message flow

## Context Access

Middlewares can access FastStream's context system:

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

This approach provides a clean, flexible way to extend FastStream applications with cross-cutting concerns while maintaining clear separation of responsibilities.
