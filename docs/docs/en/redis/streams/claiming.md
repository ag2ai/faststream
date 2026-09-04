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

!!! tip "Redis Cluster"
    `XAUTOCLAIM` works transparently with `RedisClusterBroker` — the cluster client routes all stream commands (`xgroup_create`, `xreadgroup`, `xautoclaim`) to the correct node automatically. See the [Cluster docs](../cluster.md){.internal-link}.

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

## Unified Consuming and Claiming with `XREADGROUP CLAIM`

Redis 8.4 added the [`CLAIM` option](https://redis.io/docs/latest/commands/xreadgroup/#the-claim-option){.external-link target="_blank"} to `XREADGROUP`, merging claiming and consuming into a single command: each read first claims messages that have been pending for at least the given duration, then reads new ones. FastStream exposes it via the `claim_min_idle_time` parameter, so a single subscriber can process new messages and recover abandoned ones:

```python linenums="1"
{! docs_src/redis/stream/claiming_unified.py !}
```

!!! note "Requirements"
    - **Redis server 8.4+** — older servers reject the `CLAIM` option with a `ResponseError` (`ERR syntax error`)
    - **redis-py 7.1.0+** — with an older client the subscriber fails at startup with a clear `SetupError`
    - `claim_min_idle_time` is mutually exclusive with `min_idle_time` (a different claiming mode) and with `no_ack` (never acknowledging claimed entries would redeliver them forever), and requires a consumer group with the default `last_id` (`>`), because Redis silently ignores `CLAIM` for any other id

### Claim Metadata

When `claim_min_idle_time` is set, Redis extends every returned entry with two extra fields, which FastStream exposes via the raw message:

```python
@broker.subscriber(
    stream=StreamSub("tasks", group="workers", consumer="w1", claim_min_idle_time=30000)
)
async def handler(task: str, message: RedisStreamMessage):
    message.raw_message["idle_times"][0]       # ms since the last delivery
    message.raw_message["delivery_counts"][0]  # previous deliveries (0 = new message)
```

This makes retry caps and dead-letter routing possible without an extra `XPENDING` call.

!!! warning
    `delivery_counts` counts *previous* deliveries and is therefore one less than `XPENDING`'s `times_delivered`, which includes the current delivery.

### `claim_min_idle_time` vs `min_idle_time`

The new option does not replace `XAUTOCLAIM`-based claiming — they complement each other:

| | `claim_min_idle_time` (XREADGROUP CLAIM) | `min_idle_time` (XAUTOCLAIM) |
|---|---|---|
| New messages | consumed in the same call | never consumed |
| Finding idle entries | server-side time-ordered index | cursor scan over the PEL |
| Deleted (trimmed / `XDEL`-ed) pending entries | left in the PEL | removed from the PEL |

If your stream is capped (`maxlen` / `XTRIM`), deleted entries can leave ghost records in the PEL that only `XAUTOCLAIM` cleans up.

Additional guidelines:

- The `min_idle_time` sizing warning below applies here as well: set `claim_min_idle_time` greater than your worst-case processing time, with a safety margin. This is especially important with `max_workers`, where the subscriber keeps reading while previous messages are still being processed — a too-low threshold lets it reclaim its own in-flight messages.
- Claimed entries share the `COUNT` budget with new ones and are returned first, so a large backlog of idle messages can fill whole reads. Consider setting `max_records` to bound a single delivery.
- Works with Redis Cluster in principle (each stream maps to a single node), but FastStream's cluster CI currently runs Redis 7, so this combination is not covered by tests.

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
All workers process new messages and claim abandoned ones. This pattern requires `claim_min_idle_time` (Redis 8.4+, see [Unified Consuming and Claiming](#unified-consuming-and-claiming-with-xreadgroup-claim)) — with `min_idle_time`, subscribers only claim pending messages and never consume new ones:

```python
# Each worker both processes new messages and claims abandoned ones
@broker.subscriber(
    stream=StreamSub(
        "tasks",
        group="workers",
        consumer=f"worker-{instance_id}",
        claim_min_idle_time=10000,
    )
)
async def worker(task): ...
```

## Technical Details

- **Start ID**: FastStream automatically manages the `start_id` parameter for `XAUTOCLAIM`, enabling circular scanning through the Pending Entries List
- **Empty Results**: When no pending messages meet the idle time criteria, the consumer will continue polling
- **ACK Handling**: Claimed messages must still be acknowledged using `msg.ack()` to be removed from the [PEL](https://redis.io/docs/latest/develop/data-types/streams/#working-with-multiple-consumer-groups)

## Testing

Claiming behaves the same way in tests, without waiting on real `min_idle_time` timeouts. See [Testing Consumer Groups](./testing.md){.internal-link} for details and runnable examples.

## References

For more information about Redis Streams message claiming:

- [Redis XAUTOCLAIM Documentation](https://redis.io/docs/latest/commands/xautoclaim/){.external-link target="_blank"}
- [Redis XREADGROUP CLAIM Documentation](https://redis.io/docs/latest/commands/xreadgroup/#the-claim-option){.external-link target="_blank"}
- [Redis Streams Claiming Guide](https://redis.io/docs/latest/develop/data-types/streams/#claiming-and-the-delivery-counter){.external-link target="_blank"}
