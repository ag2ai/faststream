---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis Stream Message Claiming

When working with Redis Stream Consumer Groups, there may be situations where messages remain in a pending state because a consumer failed to process them. FastStream provides a mechanism to automatically claim these pending messages using Redis's `XAUTOCLAIM` command through the `min_idle_time` parameter.

## What is Message Claiming?

In Redis Streams, when a consumer reads a message from a consumer group but fails to acknowledge it (due to a crash, network issue, or processing error), the message remains in the [**Pending Entries List (PEL)**](https://redis.io/docs/latest/develop/data-types/streams/#working-with-multiple-consumer-groups) of that consumer group. These unacknowledged messages are associated with the original consumer and have an "idle time" - the duration since they were last delivered.

Message claiming allows another consumer to take ownership of these pending messages that have been idle for too long, ensuring that messages don't get stuck and workload can be redistributed among healthy consumers.

## Using `min_idle_time` for Automatic Claiming

FastStream's `StreamSub` provides a `min_idle_time` parameter that enables automatic claiming of pending messages via Redis's `XAUTOCLAIM` command. When set, the consumer will automatically scan for and claim messages that have been pending for at least the specified duration (in milliseconds).

### Basic Example

Here's a simple example that demonstrates automatic message claiming:

```python linenums="1"
{! docs_src/redis/stream/claiming_basic.py !}
```

## How It Works

When `min_idle_time` is set:

1. **Circular Scanning**: Instead of using `XREADGROUP` to read new messages, the consumer uses `XAUTOCLAIM` to scan the Pending Entries List
2. **Idle Time Check**: Only messages that have been pending for at least `min_idle_time` milliseconds are claimed
3. **Ownership Transfer**: Claimed messages are automatically transferred from the failing consumer to the claiming consumer
4. **Continuous Processing**: The scanning process is circular - after reaching the end of the [PEL](https://redis.io/docs/latest/develop/data-types/streams/#working-with-multiple-consumer-groups), it starts over from the beginning

### Practical Use Case

Consider a scenario where you have multiple workers processing orders:

```python linenums="1"
from faststream import FastStream
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)

# Worker that might fail
@broker.subscriber(
    stream=StreamSub(
        "orders",
        group="order-processors",
        consumer="worker-1",
    )
)
async def worker_that_might_fail(order_id: str):
    # Process order - might crash before acknowledging
    await process_complex_order(order_id)
    # If crash happens here, message stays pending

# Backup worker with message claiming
@broker.subscriber(
    stream=StreamSub(
        "orders",
        group="order-processors",
        consumer="worker-2",
        min_idle_time=10000,  # 10 seconds
    )
)
async def backup_worker(order_id: str):
    # This worker will automatically pick up messages
    # that worker-1 failed to process within 10 seconds
    print(f"Recovering and processing order: {order_id}")
    await process_complex_order(order_id)
```

## Combining with Manual Acknowledgment

You can combine `min_idle_time` with manual acknowledgment policies for fine-grained control:

```python linenums="1"
{! docs_src/redis/stream/claiming_manual_ack.py !}
```

## Configuration Guidelines

### Choosing `min_idle_time`

The appropriate `min_idle_time` value depends on your use case:

- **Short duration (1-5 seconds)**: For fast-processing tasks where quick failure recovery is needed
- **Medium duration (10-60 seconds)**: For most general-purpose applications with moderate processing times
- **Long duration (5-30 minutes)**: For long-running tasks where you want to ensure a consumer has truly failed

!!! warning
    Setting `min_idle_time` too low may cause messages to be unnecessarily transferred between healthy consumers. Set it based on your typical message processing time plus a safety buffer.

### Deployment Patterns

#### Pattern 1: Dedicated Claiming Worker
Deploy a separate worker specifically for claiming abandoned messages:

```python
# Main workers (fast path)
@broker.subscriber(
    stream=StreamSub("tasks", group="workers", consumer="main-1")
)
async def main_worker(task): ...

# Claiming worker (recovery path)
@broker.subscriber(
    stream=StreamSub("tasks", group="workers", consumer="claimer", min_idle_time=15000)
)
async def claiming_worker(task): ...
```

#### Pattern 2: All Workers Can Claim
All workers can claim messages from each other:

```python
# Each worker can both process new messages and claim abandoned ones
@broker.subscriber(
    stream=StreamSub(
        "tasks",
        group="workers",
        consumer=f"worker-{instance_id}",
        min_idle_time=10000,
    )
)
async def worker(task): ...
```

## Technical Details

- **Start ID**: FastStream automatically manages the `start_id` parameter for `XAUTOCLAIM`, enabling circular scanning through the Pending Entries List
- **Empty Results**: When no pending messages meet the idle time criteria, the consumer will continue polling
- **ACK Handling**: Claimed messages must still be acknowledged using `msg.ack()` to be removed from the [PEL](https://redis.io/docs/latest/develop/data-types/streams/#working-with-multiple-consumer-groups)

## References

For more information about Redis Streams message claiming:

- [Redis XAUTOCLAIM Documentation](https://redis.io/docs/latest/commands/xautoclaim/){.external-link target="_blank"}
- [Redis Streams Claiming Guide](https://redis.io/docs/latest/develop/data-types/streams/#claiming-and-the-delivery-counter){.external-link target="_blank"}
