---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Subscribing to Google Cloud Pub/Sub

Subscribing to messages from Google Cloud Pub/Sub topics is a core feature of FastStream's GCPBroker. This section covers various subscription patterns and configurations.

## Basic Subscription

The simplest way to subscribe to messages from a Pub/Sub topic:

```python
from faststream import FastStream, Logger
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="your-project-id")
app = FastStream(broker)

@broker.subscriber("my-subscription", topic="my-topic")
async def handle_message(msg: str, logger: Logger):
    logger.info(f"Received: {msg}")
    # Process the message
```

!!! note
    The subscription name must be unique within your Google Cloud project. If the subscription doesn't exist, it will be created automatically when the broker starts.

## Message Types and Parsing

FastStream automatically deserializes messages based on their content:

```python
# String messages
@broker.subscriber("text-sub", topic="text-topic")
async def handle_text(msg: str):
    print(f"Text message: {msg}")

# JSON messages (dict)
@broker.subscriber("json-sub", topic="json-topic")
async def handle_json(msg: dict):
    print(f"JSON message: {msg}")

# Pydantic models
from pydantic import BaseModel

class UserEvent(BaseModel):
    user_id: str
    action: str
    timestamp: float

@broker.subscriber("user-sub", topic="user-events")
async def handle_user_event(msg: UserEvent):
    print(f"User {msg.user_id} performed {msg.action}")
```

## Accessing Message Attributes

Access message metadata and attributes in your handlers:

```python
from faststream.gcp import (
    MessageAttributes,
    MessageId,
    PublishTime,
    OrderingKey
)

@broker.subscriber("detailed-sub", topic="detailed-topic")
async def handle_with_metadata(
    msg: dict,
    message_id: MessageId,
    publish_time: PublishTime,
    attributes: MessageAttributes,
    ordering_key: OrderingKey
):
    print(f"Message ID: {message_id}")
    print(f"Published at: {publish_time}")
    print(f"Attributes: {attributes}")
    print(f"Ordering key: {ordering_key}")

    # Access specific attributes
    priority = attributes.get("priority", "normal")
    source = attributes.get("source", "unknown")

    if priority == "high":
        await process_urgent(msg)
```

## Custom Attribute Injection

Define custom attribute extractors for cleaner code:

```python
from typing import Annotated
from faststream import Context

# Define custom extractors
def get_user_id(attrs: MessageAttributes) -> str:
    user_id = attrs.get("user_id")
    if not user_id:
        raise ValueError("user_id attribute is required")
    return user_id

UserID = Annotated[str, Context(get_user_id)]

# Use in handlers
@broker.subscriber("user-actions-sub", topic="user-actions")
async def handle_user_action(
    msg: dict,
    user_id: UserID  # Automatically extracted from attributes
):
    print(f"Processing action for user: {user_id}")
```

## Subscription Configuration

Configure subscription behavior with `SubscriberConfig`:

```python
from faststream.gcp import SubscriberConfig

@broker.subscriber(
    "configured-sub",
    topic="events",
    config=SubscriberConfig(
        ack_deadline_seconds=60,  # Time to process before redelivery
        max_messages=10,          # Max messages per pull
        enable_message_ordering=True,  # Enable ordering
        enable_exactly_once_delivery=False,  # At-least-once delivery
        filter="attributes.priority='high'",  # Message filtering
        retry_policy={
            "minimum_backoff": "10s",
            "maximum_backoff": "600s"
        }
    )
)
async def handle_configured(msg: dict):
    # Handle with custom configuration
    pass
```

## Pull Subscription Pattern

Control message pulling behavior:

```python
@broker.subscriber(
    "pull-sub",
    topic="events",
    config=SubscriberConfig(
        max_messages=100,  # Pull up to 100 messages
        timeout=30.0       # Wait up to 30 seconds for messages
    )
)
async def handle_pulled_messages(msg: dict):
    # Process pulled messages
    pass
```

## Message Filtering

Filter messages at the subscription level:

```python
# Filter by attributes
@broker.subscriber(
    "high-priority-sub",
    topic="events",
    config=SubscriberConfig(
        filter="attributes.priority='high' AND attributes.region='us-east'"
    )
)
async def handle_high_priority(msg: dict):
    # Only receives high priority messages from us-east
    pass

# Filter by message body (requires specific setup)
@broker.subscriber(
    "error-sub",
    topic="logs",
    config=SubscriberConfig(
        filter="hasPrefix(attributes.level, 'ERROR')"
    )
)
async def handle_errors(msg: dict):
    # Only receives error-level logs
    pass
```

## Dead Letter Queues

Configure dead letter topics for failed messages:

```python
@broker.subscriber(
    "processing-sub",
    topic="tasks",
    config=SubscriberConfig(
        dead_letter_policy={
            "dead_letter_topic": "projects/your-project/topics/task-dlq",
            "max_delivery_attempts": 5
        }
    )
)
async def handle_task(msg: dict):
    # After 5 failed attempts, message goes to DLQ
    if not validate_task(msg):
        raise ValueError("Invalid task")

    await process_task(msg)
```

## Message Processing Configuration

Configure how messages are pulled and processed:

```python
@broker.subscriber(
    "configured-sub",
    topic="jobs",
    config=SubscriberConfig(
        max_messages=10,  # Pull up to 10 messages at a time from Pub/Sub
        timeout=30.0      # Timeout for pulling messages
    )
)
async def handle_message(msg: dict):
    # Each message is processed individually
    # even though multiple messages may be pulled at once
    await process_job(msg)
```

!!! note
    The `max_messages` parameter controls how many messages are pulled from Pub/Sub at once for efficiency, but each message is still processed individually by your handler function. FastStream GCP broker does not currently support batch processing where a handler receives multiple messages at once.

## Subscriber Middleware

Add custom middleware to subscribers:

```python
from faststream import BaseMiddleware

class LoggingMiddleware(BaseMiddleware):
    async def on_receive(self, message):
        logger.info(f"Received message: {message}")
        return await super().on_receive(message)

    async def on_publish(self, message):
        logger.info(f"Publishing response: {message}")
        return await super().on_publish(message)

@broker.subscriber(
    "middleware-sub",
    topic="events",
    middlewares=[LoggingMiddleware()]
)
async def handle_with_middleware(msg: dict):
    return {"processed": msg}
```

## Error Handling

Handle errors in message processing:

```python
@broker.subscriber("error-handling-sub", topic="risky-events")
async def handle_with_errors(msg: dict, logger: Logger):
    try:
        # Risky operation
        result = await risky_operation(msg)
        return {"success": result}

    except ValidationError as e:
        # Log and acknowledge (won't retry)
        logger.error(f"Validation failed: {e}")
        return {"error": "validation_failed"}

    except TemporaryError as e:
        # Don't acknowledge - message will be redelivered
        logger.warning(f"Temporary error, will retry: {e}")
        raise  # Re-raise to trigger redelivery

    except Exception as e:
        # Log and send to DLQ after max retries
        logger.error(f"Unexpected error: {e}")
        raise
```

## Subscription Groups

Group related subscribers:

```python
from faststream.gcp import GCPRouter

# Create a router for user-related events
user_router = GCPRouter()

@user_router.subscriber("user-created-sub", topic="user-created")
async def handle_user_created(user: dict):
    await create_user_profile(user)

@user_router.subscriber("user-updated-sub", topic="user-updated")
async def handle_user_updated(user: dict):
    await update_user_profile(user)

@user_router.subscriber("user-deleted-sub", topic="user-deleted")
async def handle_user_deleted(user_id: str):
    await delete_user_profile(user_id)

# Include router in main broker
broker.include_router(user_router)
```

## Testing Subscribers

Test your subscribers using TestGCPBroker:

```python
import pytest
from faststream.gcp import TestGCPBroker

@pytest.mark.asyncio
async def test_message_handler():
    async with TestGCPBroker(broker) as test_broker:
        # Publish test message
        await test_broker.publish(
            {"test": "data"},
            topic="my-topic",
            attributes={"priority": "high"}
        )

        # Verify handler was called
        handle_message.mock.assert_called_once()

        # Check the arguments
        call_args = handle_message.mock.call_args
        assert call_args[0][0] == {"test": "data"}
```

## Monitoring Subscriptions

Monitor subscription health and performance:

```python
from datetime import datetime
from collections import deque

class SubscriptionMonitor:
    def __init__(self, max_history: int = 1000):
        self.message_count = 0
        self.error_count = 0
        self.processing_times = deque(maxlen=max_history)
        self.last_message_time = None

    async def monitored_handler(self, msg: dict):
        start_time = datetime.now()
        self.last_message_time = start_time

        try:
            result = await process_message(msg)
            self.message_count += 1
            return result

        except Exception as e:
            self.error_count += 1
            raise

        finally:
            duration = (datetime.now() - start_time).total_seconds()
            self.processing_times.append(duration)

    @property
    def avg_processing_time(self):
        if not self.processing_times:
            return 0
        return sum(self.processing_times) / len(self.processing_times)

    @property
    def error_rate(self):
        total = self.message_count + self.error_count
        return self.error_count / total if total > 0 else 0

monitor = SubscriptionMonitor()

@broker.subscriber("monitored-sub", topic="events")
async def handle_monitored(msg: dict):
    return await monitor.monitored_handler(msg)
```

## Best Practices

1. **Use appropriate acknowledgment deadlines** based on processing time
2. **Implement idempotent handlers** to handle redelivered messages
3. **Use dead letter queues** for messages that repeatedly fail
4. **Monitor subscription metrics** to detect issues early
5. **Filter messages at subscription level** to reduce processing overhead
6. **Handle errors gracefully** with appropriate retry strategies
7. **Use ordering keys** when message order matters
8. **Test subscribers thoroughly** including error scenarios

## Next Steps

- Explore [Message Acknowledgment](../ack.md) strategies
- Read about [Message Attributes](../message.md) for metadata handling
- Learn about [Security Configuration](../security.md) for authentication
