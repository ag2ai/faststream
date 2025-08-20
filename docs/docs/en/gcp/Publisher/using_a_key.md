---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publishing with Ordering Keys

Ordering keys in Google Cloud Pub/Sub ensure that messages with the same key are delivered to subscribers in the order they were published. This is crucial for maintaining event sequence integrity in distributed systems.

## Understanding Ordering Keys

Ordering keys provide:

- **Guaranteed Order**: Messages with the same key are delivered in publication order
- **Parallel Processing**: Messages with different keys can be processed in parallel
- **Scalability**: Distribute load while maintaining order for related messages
- **Reliability**: Order is preserved even with retries and acknowledgments

!!! warning
    To use ordering keys, the subscription must have message ordering enabled. This is configured when creating the subscription in Google Cloud Console or via API.

## Basic Usage

Publish messages with an ordering key:

```python
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="your-project-id")

# Messages with the same ordering key are delivered in order
await broker.publish(
    {"event": "user_created", "user_id": "123"},
    topic="user-events",
    ordering_key="user-123"
)

await broker.publish(
    {"event": "profile_updated", "user_id": "123"},
    topic="user-events",
    ordering_key="user-123"
)

await broker.publish(
    {"event": "subscription_added", "user_id": "123"},
    topic="user-events",
    ordering_key="user-123"
)
```

## Using Ordering Keys with Publisher Decorator

Configure default ordering keys for publishers:

```python
from faststream.gcp import PublisherConfig

@broker.publisher(
    "ordered-events",
    config=PublisherConfig(
        ordering_key="default-order"
    )
)
async def publish_ordered(data: dict):
    return data

# Override the default ordering key
@broker.subscriber("commands-sub", topic="commands")
async def handle_command(cmd: dict):
    user_id = cmd.get("user_id")

    # This will use ordering key "user-{user_id}"
    await broker.publish(
        {"command": cmd, "status": "processed"},
        topic="ordered-events",
        ordering_key=f"user-{user_id}"
    )
```

## Dynamic Ordering Keys

Generate ordering keys based on message content:

```python
from faststream.gcp import ResponseOrderingKey, GCPResponse

@broker.subscriber("transactions-sub", topic="transactions")
@broker.publisher("transaction-results")
async def process_transaction(
    transaction: dict
) -> GCPResponse:
    account_id = transaction["account_id"]
    result = await process_payment(transaction)

    # Use account ID as ordering key to maintain transaction order
    return GCPResponse(
        data=result,
        ordering_key=ResponseOrderingKey(account_id)
    )
```

## Common Ordering Key Patterns

### User-Based Ordering

Maintain order for user-specific events:

```python
async def publish_user_event(user_id: str, event: dict):
    """Publish user events with user-based ordering."""
    await broker.publish(
        event,
        topic="user-events",
        ordering_key=f"user:{user_id}"
    )

# Usage
await publish_user_event("123", {"type": "login"})
await publish_user_event("123", {"type": "settings_changed"})
await publish_user_event("123", {"type": "logout"})
```

### Session-Based Ordering

Maintain order within user sessions:

```python
async def publish_session_event(session_id: str, event: dict):
    """Publish events ordered by session."""
    await broker.publish(
        {**event, "session_id": session_id},
        topic="session-events",
        ordering_key=f"session:{session_id}"
    )

# Different sessions can be processed in parallel
await publish_session_event("sess-abc", {"action": "page_view"})
await publish_session_event("sess-xyz", {"action": "click"})
await publish_session_event("sess-abc", {"action": "form_submit"})
```

### Entity-Based Ordering

Maintain order for domain entities:

```python
class OrderEventPublisher:
    def __init__(self, broker: GCPBroker):
        self.broker = broker

    async def publish_order_event(
        self,
        order_id: str,
        event_type: str,
        data: dict
    ):
        """Publish order events with order-based ordering."""
        await self.broker.publish(
            {
                "order_id": order_id,
                "event_type": event_type,
                "data": data,
                "timestamp": datetime.utcnow().isoformat()
            },
            topic="order-events",
            ordering_key=f"order:{order_id}"
        )

# Usage
publisher = OrderEventPublisher(broker)
await publisher.publish_order_event("ORD-123", "created", {...})
await publisher.publish_order_event("ORD-123", "payment_received", {...})
await publisher.publish_order_event("ORD-123", "shipped", {...})
```

## Hierarchical Ordering Keys

Use composite keys for complex ordering requirements:

```python
async def publish_tenant_user_event(
    tenant_id: str,
    user_id: str,
    event: dict
):
    """Maintain order per user within a tenant."""
    ordering_key = f"tenant:{tenant_id}:user:{user_id}"

    await broker.publish(
        {
            "tenant_id": tenant_id,
            "user_id": user_id,
            **event
        },
        topic="multi-tenant-events",
        ordering_key=ordering_key
    )
```

## Handling Ordering Key Failures

When a message with an ordering key fails, subsequent messages with the same key are blocked:

```python
from google.api_core import exceptions

async def publish_with_ordering_recovery(
    broker: GCPBroker,
    message: dict,
    topic: str,
    ordering_key: str
):
    """Publish with ordering key and handle failures."""
    try:
        await broker.publish(
            message,
            topic=topic,
            ordering_key=ordering_key
        )
    except exceptions.FailedPrecondition as e:
        # Ordering key is in error state
        logger.error(f"Ordering key {ordering_key} is blocked: {e}")

        # Option 1: Resume publishing with this key
        # (requires enabling resumption in Pub/Sub)

        # Option 2: Use a different ordering key
        new_key = f"{ordering_key}-retry-{datetime.now().timestamp()}"
        await broker.publish(
            message,
            topic=topic,
            ordering_key=new_key
        )
```

## Publishing Multiple Ordered Messages

When publishing multiple messages with ordering keys:

```python
# Publish multiple messages in order
user_events = [
    {"action": "click", "timestamp": 1},
    {"action": "view", "timestamp": 2},
    {"action": "purchase", "timestamp": 3}
]

# Publish each message individually but in order
for event in user_events:
    await broker.publish(
        event,
        topic="user-events",
        ordering_key="user-123"
    )
```

!!! note
    Even though each `publish()` call is individual, messages with the same ordering key will be delivered to subscribers in the order they were published.

## Testing Ordered Publishing

Test ordering key behavior:

```python
import pytest
from faststream.gcp import TestGCPBroker

@pytest.mark.asyncio
async def test_ordering_keys():
    async with TestGCPBroker(broker) as test_broker:
        # Publish messages with ordering keys
        await test_broker.publish(
            {"seq": 1},
            "test-topic",
            ordering_key="test-key"
        )
        await test_broker.publish(
            {"seq": 2},
            "test-topic",
            ordering_key="test-key"
        )

        # Verify ordering keys
        messages = test_broker.published_messages
        assert len(messages) == 2
        assert all(msg.ordering_key == "test-key" for msg in messages)
        assert messages[0].data["seq"] == 1
        assert messages[1].data["seq"] == 2
```

## Monitoring and Debugging

Track ordering key usage:

```python
from collections import defaultdict
from datetime import datetime

class OrderingKeyMonitor:
    def __init__(self):
        self.key_counts = defaultdict(int)
        self.key_last_used = {}
        self.key_errors = defaultdict(int)

    async def publish_monitored(
        self,
        broker: GCPBroker,
        message: dict,
        topic: str,
        ordering_key: str
    ):
        """Publish with monitoring."""
        try:
            result = await broker.publish(
                message,
                topic=topic,
                ordering_key=ordering_key
            )

            # Track successful publish
            self.key_counts[ordering_key] += 1
            self.key_last_used[ordering_key] = datetime.now()

            return result

        except Exception as e:
            # Track errors
            self.key_errors[ordering_key] += 1
            logger.error(
                f"Failed to publish with key {ordering_key}: {e}",
                extra={
                    "ordering_key": ordering_key,
                    "error_count": self.key_errors[ordering_key]
                }
            )
            raise

    def get_stats(self):
        """Get ordering key statistics."""
        return {
            "total_keys": len(self.key_counts),
            "total_messages": sum(self.key_counts.values()),
            "keys_with_errors": len(self.key_errors),
            "most_used_key": max(self.key_counts, key=self.key_counts.get)
            if self.key_counts else None
        }
```

## Best Practices

1. **Choose Keys Wisely**: Use natural business identifiers (user_id, order_id, session_id)
2. **Limit Key Cardinality**: Too many unique keys can impact performance
3. **Handle Failures**: Implement recovery strategies for blocked ordering keys
4. **Monitor Key Distribution**: Ensure even distribution to avoid hot spots
5. **Document Key Schemes**: Clearly document your ordering key strategy
6. **Test Order Preservation**: Verify ordering in integration tests
7. **Consider Partitioning**: Use ordering keys to partition workload effectively

## Performance Considerations

- **Throughput**: Each ordering key is limited to ~1000 messages/second
- **Parallelism**: Different ordering keys can be processed in parallel
- **Memory**: Pub/Sub maintains order state per key (consider key lifecycle)
- **Latency**: Ordering may introduce slight latency for guaranteed delivery

## Next Steps

- Explore [Subscriber Configuration](../Subscriber/index.md) for ordered message consumption
- Read about [Message Attributes](../message.md) for additional message metadata
- Learn about [Message Acknowledgment](../ack.md) for reliable processing
