---
description: >-
  Handle AIOKafka message processing errors with subscriber, broker,
  and FastStream exception handlers.
---

# Exception Handlers

With AIOKafka, you can register an `exception_handler` on a subscriber,
on the broker, or on the `FastStream` application.

Handlers can be synchronous or asynchronous. Each handler receives the
exception and returns a boolean.

## Handler Order

FastStream calls the handlers in this order:

1. The subscriber's handler.
2. The broker's handler.
3. The application's handler.

Missing handlers are skipped. Processing stops when a handler returns `True`.

## Return Value

- `True`: the exception is handled and suppressed.
- `False`: processing continues with the next handler.

If no handler returns `True`, the original exception is raised again.

## Example

```python linenums="1"
{!> docs_src/getting_started/exception_handlers/kafka.py !}
```

Publish one of these messages to the `exception-handlers` topic:

| Message | Exception | Result |
|---------|-----------|--------|
| `subscriber` | `ValueError` | Suppressed by the subscriber's handler |
| `broker` | `RuntimeError` | Suppressed by the broker's handler |
| `application` | `TypeError` | Logged by the application's handler and raised again |

## Context

Exception handlers support [context injection](../context.md){.internal-link}.
The application handler in the example uses `Logger` to access the logger
for the current message.

## Middleware and Acknowledgement

If middleware has already suppressed an exception, the exception handlers
are not called.

Returning `True` controls exception propagation. Message acknowledgement
follows the broker's [acknowledgement policy](../acknowledgement.md){.internal-link}.

`StopConsume` and `StopApplication` continue through the normal shutdown
handling and are not passed to these callbacks.