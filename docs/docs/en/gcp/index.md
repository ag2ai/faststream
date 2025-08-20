---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Google Cloud Pub/Sub Routing

## Google Cloud Pub/Sub

Google Cloud Pub/Sub is a fully-managed real-time messaging service that allows you to send and receive messages between independent applications. It provides reliable, many-to-many, asynchronous messaging between applications and can be used to decouple systems and components.

Key features of Google Cloud Pub/Sub include:

- **Scalability**: Automatically scales to handle millions of messages per second
- **Reliability**: Guarantees at-least-once message delivery
- **Global**: Available in all Google Cloud regions
- **Push and Pull**: Supports both push and pull subscription models
- **Message Ordering**: Maintains message order with ordering keys
- **Message Attributes**: Attach metadata to messages for filtering and routing

## FastStream GCPBroker

The FastStream GCPBroker is a key component that enables seamless integration with Google Cloud Pub/Sub. It provides a simple and intuitive API for publishing and consuming messages from Pub/Sub topics, with support for all Pub/Sub features including message attributes, ordering keys, and acknowledgment handling.

### Installation

To use GCPBroker, you need to install FastStream with GCP support:

```bash
pip install "faststream[gcp]"
```

### Establishing a Connection

To connect to Google Cloud Pub/Sub using the FastStream GCPBroker module, follow these steps:

1. **Initialize the GCPBroker instance:** Start by initializing a GCPBroker instance with your Google Cloud project configuration.

2. **Create your processing logic:** Write functions that will consume incoming messages and optionally produce responses.

3. **Decorate your processing functions:** Use `#!python @broker.subscriber(...)` to connect your functions to Pub/Sub subscriptions.

Here's a basic example demonstrating how to establish a connection:

```python linenums="1"
import os
from faststream import FastStream, Logger
from faststream.gcp import GCPBroker

# Initialize the broker
broker = GCPBroker(
    project_id=os.getenv("GCP_PROJECT_ID", "your-project-id"),
    # Optional: Use emulator for local development
    emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
)

app = FastStream(broker)

@broker.subscriber("test-subscription", topic="test-topic")
async def handle_message(msg: str, logger: Logger):
    logger.info(f"Received message: {msg}")
    # Process the message
    return {"status": "processed", "original": msg}

@app.after_startup
async def publish_example():
    # Publish a test message after startup
    await broker.publish("Hello, Pub/Sub!", "test-topic")
```

### Key Features

#### Message Attributes

GCP Pub/Sub allows attaching key-value metadata to messages. FastStream provides comprehensive support for message attributes:

```python
from faststream.gcp import MessageAttributes, OrderingKey

@broker.subscriber("events-sub", topic="events")
async def handle_event(
    msg: dict,
    attrs: MessageAttributes,  # Access all attributes
    ordering_key: OrderingKey,  # Access ordering key
):
    event_type = attrs.get("type", "unknown")
    priority = attrs.get("priority", "normal")

    # Process based on attributes
    if priority == "high":
        await process_immediately(msg)
    else:
        await queue_for_later(msg)

# Publishing with attributes
await broker.publish(
    {"data": "event"},
    topic="events",
    attributes={"type": "user_action", "priority": "high"},
    ordering_key="user-123"
)
```

#### Message Acknowledgment

Control message acknowledgment behavior for reliable processing:

```python
@broker.subscriber(
    "reliable-sub",
    topic="important-events",
    auto_ack=False  # Manual acknowledgment
)
async def handle_important(msg: dict):
    try:
        # Process the message
        result = await process_critical_operation(msg)
        # Manually acknowledge on success
        await msg.ack()
    except Exception as e:
        # Message will be redelivered if not acknowledged
        logger.error(f"Failed to process: {e}")
        await msg.nack()  # Explicit negative acknowledgment
```

#### Testing Support

FastStream provides a TestGCPBroker for testing your Pub/Sub applications:

```python
import pytest
from faststream.gcp import TestGCPBroker

@pytest.mark.asyncio
async def test_message_handling():
    async with TestGCPBroker(broker) as test_broker:
        # Publish a test message
        await test_broker.publish("test data", "test-topic")

        # Verify the handler was called
        handle_message.mock.assert_called_once_with("test data")
```

### Advanced Configuration

#### Broker Configuration

```python
from faststream.gcp import GCPBroker, GCPSecurity

broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials_path="/path/to/service-account.json"
    ),
    emulator_host="localhost:8085",  # For local development
    default_topic_config={
        "message_retention_duration": "7d",
        "labels": {"environment": "production"}
    },
    default_subscription_config={
        "ack_deadline_seconds": 60,
        "enable_message_ordering": True
    }
)
```

#### Publisher Configuration

```python
from faststream.gcp import PublisherConfig

@broker.publisher(
    "output-topic",
    config=PublisherConfig(
        ordering_key="default-key",
        attributes={"source": "app-name"}
    )
)
async def publish_result(data: dict):
    return data
```

#### Retry Configuration

```python
from faststream.gcp import RetryConfig

@broker.subscriber(
    "retry-sub",
    topic="events",
    retry_config=RetryConfig(
        maximum_backoff=60.0,
        minimum_backoff=1.0,
        maximum_doublings=5
    )
)
async def handle_with_retry(msg: str):
    # Automatic retry on failure
    pass
```

### Integration with FastAPI

FastStream GCPBroker integrates seamlessly with FastAPI:

```python
from fastapi import FastAPI
from faststream.gcp.fastapi import GCPRouter

# Create FastAPI app
api_app = FastAPI()

# Create GCP router
router = GCPRouter(project_id="your-project-id")

@router.subscriber("api-events-sub", topic="api-events")
async def handle_api_event(msg: dict):
    # Process events from API
    return {"processed": True}

# Include router in FastAPI
api_app.include_router(router)
```

### Environment Variables

GCPBroker supports configuration through environment variables:

- `GOOGLE_CLOUD_PROJECT` or `GCP_PROJECT_ID`: Sets the default project ID
- `PUBSUB_EMULATOR_HOST`: Connects to Pub/Sub emulator for local development
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account credentials

### Best Practices

1. **Use the Pub/Sub emulator for local development** to avoid costs and ensure isolation
2. **Implement proper error handling** and use manual acknowledgment for critical messages
3. **Use message attributes** for routing and filtering instead of parsing message bodies
4. **Set appropriate acknowledgment deadlines** based on your processing time
5. **Use ordering keys** when message order matters for a specific entity
6. **Monitor dead letter queues** for messages that couldn't be processed
7. **Use appropriate message attributes** for routing and filtering instead of parsing message bodies

For more examples and detailed API documentation, explore the other sections of the GCP documentation.
