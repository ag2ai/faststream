---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publishing to Google Cloud Pub/Sub

Publishing messages to Google Cloud Pub/Sub topics is a fundamental operation in FastStream's GCPBroker. This section covers various publishing patterns and configurations.

## Basic Publishing

The simplest way to publish messages to a Pub/Sub topic:

```python
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="your-project-id")

# Publish a simple string message
await broker.publish("Hello, World!", topic="my-topic")

# Publish a dictionary (automatically serialized to JSON)
await broker.publish(
    {"user": "john", "action": "login"},
    topic="events"
)
```

## Publisher Decorator

Use the `@broker.publisher()` decorator to create reusable publishers:

```python
from faststream import FastStream
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="your-project-id")
app = FastStream(broker)

@broker.publisher("processed-events")
async def publish_processed(data: dict):
    return {"processed": True, "data": data}

@broker.subscriber("raw-events-sub", topic="raw-events")
async def handle_event(msg: dict):
    # Process the event
    result = await process_event(msg)
    # Publish to another topic
    await publish_processed(result)
```

## Publishing with Message Attributes

Message attributes provide metadata about your messages without adding to the message payload:

```python
# Direct publishing with attributes
await broker.publish(
    message={"order_id": "12345", "amount": 99.99},
    topic="orders",
    attributes={
        "region": "us-west",
        "priority": "high",
        "customer_tier": "premium"
    }
)

# Using publisher decorator with default attributes
from faststream.gcp import PublisherConfig

@broker.publisher(
    "notifications",
    config=PublisherConfig(
        attributes={"source": "order-service", "version": "1.0"}
    )
)
async def publish_notification(msg: dict):
    return msg
```

## Publishing with Ordering Keys

Ordering keys ensure messages with the same key are delivered in order:

```python
# Publish messages with ordering key
user_id = "user-123"

# These messages will be delivered in order for this user
await broker.publish(
    {"event": "login", "timestamp": "2024-01-01T10:00:00"},
    topic="user-events",
    ordering_key=user_id
)

await broker.publish(
    {"event": "profile_update", "timestamp": "2024-01-01T10:05:00"},
    topic="user-events",
    ordering_key=user_id
)

await broker.publish(
    {"event": "logout", "timestamp": "2024-01-01T11:00:00"},
    topic="user-events",
    ordering_key=user_id
)
```

## Response Publishing

Automatically publish handler responses to another topic:

```python
@broker.subscriber("requests-sub", topic="requests")
@broker.publisher("responses")
async def handle_request(msg: dict) -> dict:
    # Process the request
    result = await process_request(msg)
    # The return value is automatically published to "responses" topic
    return {
        "request_id": msg.get("id"),
        "result": result,
        "status": "completed"
    }
```

## Advanced Response Publishing with Attributes

You can return both data and metadata from handlers:

```python
from faststream.gcp import GCPResponse, ResponseAttributes, ResponseOrderingKey

@broker.subscriber("commands-sub", topic="commands")
@broker.publisher("results")
async def handle_command(
    cmd: dict,
    attrs: MessageAttributes
) -> GCPResponse:
    # Process command
    result = await execute_command(cmd)

    # Return response with attributes
    return GCPResponse(
        data=result,
        attributes=ResponseAttributes({
            "command_type": attrs.get("type"),
            "execution_time": "125ms",
            "status": "success"
        }),
        ordering_key=ResponseOrderingKey(f"cmd-{cmd['id']}")
    )
```

## Publishing Multiple Messages

For publishing multiple messages, use individual publish calls:

```python
# Publish multiple messages
messages = [
    {"id": 1, "data": "first"},
    {"id": 2, "data": "second"},
    {"id": 3, "data": "third"}
]

# Publish each message individually
for msg in messages:
    await broker.publish(
        msg,
        topic="messages-topic",
        attributes={"batch_id": "batch-001"}
    )
```

!!! note
    The GCP broker has a `publish_batch` method, but it currently just calls individual `publish()` operations internally rather than using true batch publishing.

## Publisher Configuration

Configure publisher behavior with `PublisherConfig`:

```python
from faststream.gcp import PublisherConfig

@broker.publisher(
    "configured-topic",
    config=PublisherConfig(
        ordering_key="default-key",
        attributes={
            "environment": "production",
            "service": "api-gateway"
        },
        timeout=30.0  # Publishing timeout in seconds
    )
)
async def publish_configured(data: dict):
    return data
```

## Error Handling

Handle publishing errors gracefully:

```python
from google.api_core import exceptions

async def safe_publish(broker, message, topic):
    try:
        message_id = await broker.publish(message, topic)
        logger.info(f"Published message with ID: {message_id}")
        return message_id
    except exceptions.NotFound:
        logger.error(f"Topic {topic} not found")
        # Create topic or handle error
    except exceptions.DeadlineExceeded:
        logger.error("Publishing timeout")
        # Retry or handle timeout
    except Exception as e:
        logger.error(f"Publishing failed: {e}")
        # Handle other errors
```

## Publishing Patterns

### Request-Reply Pattern

```python
import uuid
from asyncio import create_task, wait_for

# Store pending requests
pending_requests = {}

@broker.subscriber("replies-sub", topic="replies")
async def handle_reply(msg: dict, attrs: MessageAttributes):
    request_id = attrs.get("request_id")
    if request_id in pending_requests:
        pending_requests[request_id].set_result(msg)

async def request_with_reply(data: dict, timeout: float = 5.0):
    request_id = str(uuid.uuid4())

    # Create future for response
    future = asyncio.Future()
    pending_requests[request_id] = future

    # Publish request
    await broker.publish(
        data,
        topic="requests",
        attributes={"request_id": request_id, "reply_to": "replies"}
    )

    try:
        # Wait for reply
        response = await wait_for(future, timeout=timeout)
        return response
    finally:
        pending_requests.pop(request_id, None)
```

### Event Sourcing Pattern

```python
from datetime import datetime

@broker.publisher("event-store")
async def publish_event(
    aggregate_id: str,
    event_type: str,
    event_data: dict
) -> dict:
    return {
        "aggregate_id": aggregate_id,
        "event_type": event_type,
        "event_data": event_data,
        "timestamp": datetime.utcnow().isoformat(),
        "version": 1
    }

# Usage
await publish_event(
    aggregate_id="order-123",
    event_type="OrderCreated",
    event_data={"customer": "john", "items": [...]}
)
```

## Testing Publishers

Test your publishers using TestGCPBroker:

```python
import pytest
from faststream.gcp import TestGCPBroker

@pytest.mark.asyncio
async def test_publisher():
    async with TestGCPBroker(broker) as test_broker:
        # Test direct publishing
        await test_broker.publish("test message", "test-topic")

        # Verify message was published
        assert test_broker.published_messages

        msg = test_broker.published_messages[0]
        assert msg.data == "test message"
        assert msg.topic == "test-topic"

@pytest.mark.asyncio
async def test_publisher_with_attributes():
    async with TestGCPBroker(broker) as test_broker:
        await test_broker.publish(
            "test",
            "topic",
            attributes={"key": "value"}
        )

        msg = test_broker.published_messages[0]
        assert msg.attributes == {"key": "value"}
```

## Best Practices

1. **Use attributes for metadata** instead of including it in the message body
2. **Implement retry logic** for transient failures
3. **Use ordering keys consistently** for related messages
4. **Monitor publishing metrics** to detect issues early
5. **Use message attributes** for routing and metadata instead of message body
6. **Set appropriate timeouts** based on your use case
7. **Validate messages** before publishing to avoid downstream issues
8. **Use structured logging** to track published messages

## Next Steps

- Explore [Publishing with Keys](using_a_key.md) for message ordering
- Read about [Subscribers](../Subscriber/index.md) to consume published messages
- Learn about [Message Attributes](../message.md) for metadata handling
