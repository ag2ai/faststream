---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# GCP Pub/Sub Messages and Attributes

Google Cloud Pub/Sub messages consist of data payload and optional attributes (key-value metadata). FastStream provides comprehensive support for working with both message content and attributes.

## Message Structure

A Pub/Sub message contains:

- **Data**: The message payload (string or bytes)
- **Attributes**: Key-value pairs of metadata
- **Message ID**: Unique identifier assigned by Pub/Sub
- **Publish Time**: Timestamp when the message was published
- **Ordering Key**: Optional key for ordered delivery

## Accessing Message Components

### Basic Message Data

Access the message payload in your handlers:

```python
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="your-project-id")

# Simple string message
@broker.subscriber("text-sub", topic="text-topic")
async def handle_text(msg: str):
    print(f"Received: {msg}")

# JSON message (automatically parsed)
@broker.subscriber("json-sub", topic="json-topic")
async def handle_json(msg: dict):
    print(f"User: {msg['user']}, Action: {msg['action']}")

# Binary data
@broker.subscriber("binary-sub", topic="binary-topic")
async def handle_binary(msg: bytes):
    print(f"Received {len(msg)} bytes")
```

### Message Metadata

Access message metadata using FastStream's annotation system:

```python
from faststream.gcp import (
    MessageId,
    PublishTime,
    OrderingKey,
    MessageAttributes
)

@broker.subscriber("metadata-sub", topic="events")
async def handle_with_metadata(
    msg: dict,
    message_id: MessageId,
    publish_time: PublishTime,
    ordering_key: OrderingKey,
    attributes: MessageAttributes
):
    print(f"Message ID: {message_id}")
    print(f"Published at: {publish_time}")
    print(f"Ordering key: {ordering_key}")
    print(f"Attributes: {dict(attributes)}")
```

### Native Message Access

Access the complete native Pub/Sub message:

```python
from faststream.gcp import NativeMessage

@broker.subscriber("native-sub", topic="events")
async def handle_native(msg: NativeMessage):
    # Access all message properties
    print(f"Data: {msg.data}")
    print(f"Attributes: {msg.attributes}")
    print(f"Message ID: {msg.message_id}")
    print(f"Publish time: {msg.publish_time}")
    print(f"Ordering key: {msg.ordering_key}")

    # Manual acknowledgment with native message
    await msg.ack()
```

## Message Attributes

Attributes provide metadata without increasing message payload size. They're perfect for routing, filtering, and providing context.

### Publishing with Attributes

Add attributes when publishing messages:

```python
# Simple attributes
await broker.publish(
    "Order processed",
    topic="notifications",
    attributes={
        "order_id": "ORD-123",
        "customer_id": "CUST-456",
        "priority": "high",
        "region": "us-east"
    }
)

# Attributes with complex data (converted to strings)
from datetime import datetime

await broker.publish(
    {"status": "completed"},
    topic="events",
    attributes={
        "timestamp": datetime.now().isoformat(),
        "version": "1.2.3",
        "retry_count": "0",
        "source_system": "order-service"
    }
)
```

### Accessing Attributes in Handlers

Multiple ways to access message attributes:

```python
# Access all attributes
@broker.subscriber("all-attrs-sub", topic="events")
async def handle_all_attributes(
    msg: dict,
    attributes: MessageAttributes
):
    # MessageAttributes is a dict-like object
    for key, value in attributes.items():
        print(f"{key}: {value}")

    # Get specific attribute with default
    priority = attributes.get("priority", "normal")

# Access specific attributes using custom annotations
from typing import Annotated
from faststream import Context

def get_user_id(attrs: MessageAttributes) -> str:
    return attrs.get("user_id", "anonymous")

def get_priority(attrs: MessageAttributes) -> str:
    return attrs.get("priority", "normal")

UserId = Annotated[str, Context(get_user_id)]
Priority = Annotated[str, Context(get_priority)]

@broker.subscriber("custom-attrs-sub", topic="events")
async def handle_custom_attributes(
    msg: dict,
    user_id: UserId,
    priority: Priority
):
    print(f"User {user_id} sent {priority} priority message")
```

### Attribute Patterns

Common patterns for using attributes effectively:

```python
# Routing pattern
@broker.subscriber("routing-sub", topic="events")
async def route_by_attribute(
    msg: dict,
    attributes: MessageAttributes
):
    event_type = attributes.get("type")

    if event_type == "user_created":
        await handle_user_created(msg)
    elif event_type == "user_updated":
        await handle_user_updated(msg)
    elif event_type == "user_deleted":
        await handle_user_deleted(msg)

# Filtering pattern
@broker.subscriber(
    "filtered-sub",
    topic="events",
    config=SubscriberConfig(
        filter="attributes.priority='high' AND attributes.region='us-east'"
    )
)
async def handle_high_priority_us_east(msg: dict):
    # Only receives high priority messages from us-east
    pass

# Context enrichment pattern
@broker.subscriber("context-sub", topic="events")
async def handle_with_context(
    msg: dict,
    attributes: MessageAttributes,
    logger: Logger
):
    # Use attributes to enrich logging context
    logger.info(
        "Processing event",
        extra={
            "trace_id": attributes.get("trace_id"),
            "user_id": attributes.get("user_id"),
            "session_id": attributes.get("session_id"),
            "source": attributes.get("source")
        }
    )

    await process_with_context(msg, attributes)
```

## Advanced Attribute Usage

### Required Attributes

Ensure required attributes are present:

```python
from faststream import Context
from typing import Annotated

def require_user_id(attrs: MessageAttributes) -> str:
    user_id = attrs.get("user_id")
    if not user_id:
        raise ValueError("user_id attribute is required")
    return user_id

RequiredUserId = Annotated[str, Context(require_user_id)]

@broker.subscriber("required-attrs-sub", topic="user-events")
async def handle_with_required(
    msg: dict,
    user_id: RequiredUserId  # Will raise if not present
):
    print(f"Processing for user: {user_id}")
```

### Typed Attributes

Create typed attribute extractors:

```python
from enum import Enum
from typing import Optional

class Priority(Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"

def get_typed_priority(attrs: MessageAttributes) -> Priority:
    priority_str = attrs.get("priority", "normal")
    try:
        return Priority(priority_str)
    except ValueError:
        return Priority.NORMAL

TypedPriority = Annotated[Priority, Context(get_typed_priority)]

@broker.subscriber("typed-sub", topic="events")
async def handle_typed(
    msg: dict,
    priority: TypedPriority
):
    if priority == Priority.URGENT:
        await process_immediately(msg)
    elif priority == Priority.HIGH:
        await process_soon(msg)
    else:
        await queue_for_later(msg)
```

### Composite Attributes

Work with structured attribute data:

```python
from dataclasses import dataclass
import json

@dataclass
class UserContext:
    user_id: str
    tenant_id: str
    roles: list[str]

    @classmethod
    def from_attributes(cls, attrs: MessageAttributes) -> "UserContext":
        return cls(
            user_id=attrs.get("user_id", ""),
            tenant_id=attrs.get("tenant_id", ""),
            roles=json.loads(attrs.get("roles", "[]"))
        )

def get_user_context(attrs: MessageAttributes) -> UserContext:
    return UserContext.from_attributes(attrs)

UserCtx = Annotated[UserContext, Context(get_user_context)]

@broker.subscriber("user-context-sub", topic="events")
async def handle_with_user_context(
    msg: dict,
    user_ctx: UserCtx
):
    if "admin" in user_ctx.roles:
        await handle_admin_action(msg, user_ctx)
    else:
        await handle_user_action(msg, user_ctx)
```

## Response Attributes

Return attributes when publishing responses:

```python
from faststream.gcp import (
    GCPResponse,
    ResponseAttributes,
    ResponseOrderingKey
)

@broker.subscriber("request-sub", topic="requests")
@broker.publisher("responses")
async def handle_request(
    msg: dict,
    attributes: MessageAttributes
) -> GCPResponse:
    # Process request
    result = await process_request(msg)

    # Return response with attributes
    return GCPResponse(
        data=result,
        attributes=ResponseAttributes({
            "request_id": attributes.get("request_id"),
            "processing_time": "125ms",
            "status": "success",
            "handler_version": "1.0"
        }),
        ordering_key=ResponseOrderingKey(
            attributes.get("session_id")
        )
    )
```

## Attribute Validation

Validate attributes before processing:

```python
from pydantic import BaseModel, Field, validator

class EventAttributes(BaseModel):
    user_id: str = Field(..., min_length=1)
    event_type: str = Field(..., regex="^[a-z_]+$")
    priority: int = Field(default=1, ge=1, le=5)
    timestamp: str = Field(...)

    @validator("timestamp")
    def validate_timestamp(cls, v):
        try:
            datetime.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError("Invalid timestamp format")

def validate_attributes(attrs: MessageAttributes) -> EventAttributes:
    return EventAttributes(**attrs)

ValidatedAttrs = Annotated[EventAttributes, Context(validate_attributes)]

@broker.subscriber("validated-sub", topic="events")
async def handle_validated(
    msg: dict,
    attrs: ValidatedAttrs
):
    print(f"Valid event from {attrs.user_id} at {attrs.timestamp}")
```

## Monitoring Attributes

Track attribute usage for monitoring:

```python
from collections import defaultdict

class AttributeMonitor:
    def __init__(self):
        self.attribute_counts = defaultdict(int)
        self.attribute_values = defaultdict(set)

    def record(self, attributes: MessageAttributes):
        for key, value in attributes.items():
            self.attribute_counts[key] += 1
            self.attribute_values[key].add(value)

    def get_stats(self):
        return {
            "total_unique_keys": len(self.attribute_counts),
            "most_common_key": max(self.attribute_counts, key=self.attribute_counts.get)
            if self.attribute_counts else None,
            "attribute_cardinality": {
                key: len(values)
                for key, values in self.attribute_values.items()
            }
        }

monitor = AttributeMonitor()

@broker.subscriber("monitored-sub", topic="events")
async def handle_monitored(
    msg: dict,
    attributes: MessageAttributes
):
    monitor.record(attributes)

    # Process message
    await process_message(msg, attributes)

    # Log stats periodically
    if monitor.attribute_counts["_total"] % 1000 == 0:
        logger.info(f"Attribute stats: {monitor.get_stats()}")
```

## Best Practices

1. **Use attributes for metadata**, not data that belongs in the message body
2. **Keep attribute values small** (< 1024 bytes per value)
3. **Use consistent attribute names** across your system
4. **Document attribute schemas** for each topic
5. **Validate required attributes** early in handlers
6. **Use attributes for filtering** at the subscription level
7. **Monitor attribute cardinality** to avoid explosion
8. **Consider attribute limits** (100 attributes per message maximum)

## Attribute Limitations

- Maximum 100 attributes per message
- Attribute keys: 256 bytes maximum
- Attribute values: 1024 bytes maximum
- Total attributes size: counts toward 10MB message size limit
- Attributes are always strings (convert other types)

## Next Steps

- Learn about [Message Acknowledgment](ack.md)
- Explore [Security Configuration](security.md)
- Read about [Publisher Configuration](Publisher/index.md)
