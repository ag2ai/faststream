---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Testing Stream Consumer Groups

[Consumer Groups](./groups.md){.internal-link} and [Message Claiming](./claiming.md){.internal-link} both rely on Redis's [**Pending Entries List (PEL)**](https://redis.io/docs/latest/develop/data-types/streams/#working-with-multiple-consumer-groups){.external-link target="_blank"} to track messages that were delivered but not yet acknowledged. Exercising that lifecycle against a real Redis instance means waiting out real `min_idle_time` timers, which makes tests slow and non-deterministic.

`TestRedisBroker` avoids that by emulating the PEL in memory. It applies the same rules a real Redis consumer group would, but synchronously - so a `min_idle_time` consumer can reclaim a nacked message within the very same `#!python await broker.publish(...)` call instead of waiting for a real timeout.

## How the Emulated PEL Works

For every message delivered to a consumer group member, the fake broker applies the same three rules as production Redis:

1. **Successful processing** removes the entry from the PEL - nothing is left pending.
2. **A nacked message** (e.g. a raised `NackMessage`) leaves the entry in the PEL, where it stays until a `min_idle_time` consumer reclaims it.
3. **`#!python no_ack=True`** consumers are never tracked in the PEL at all, matching Redis's `NOACK` flag, which acknowledges a message the moment it's delivered.

## Pending Without a Claimer

If nothing in the group has `min_idle_time` set, a nacked message simply stays pending. Here, `flaky_worker` always fails, and there's no one to reclaim its work:

```python linenums="1"
{! docs_src/redis/stream/pel_pending.py !}
```

Pass a `PEL` instance to `TestRedisBroker` to inspect it directly after publishing. A `pel` fixture keeps every test working against its own instance:

```python linenums="1"
{! docs_src/redis/stream/pel_testing.py [ln:17-29] !}
```

## Multiple Groups Mean Multiple PEL Entries

The PEL is tracked per consumer group, not per message. If the same stream has several groups subscribed - each modeling an independent workload - a single published message that goes unacknowledged in every group leaves one pending entry *per group*, not one shared entry:

```python linenums="1"
{! docs_src/redis/stream/pel_multiple_groups.py !}
```

```python linenums="1"
{! docs_src/redis/stream/pel_testing.py [ln:32-40] !}
```

This mirrors real Redis: `XPENDING` is scoped to a single consumer group, so groups never see or interfere with each other's pending entries, even when they're reading the same stream.

## Reclaiming a Pending Message

Add a `min_idle_time` consumer to the same group, and it reclaims the pending entry once `flaky_worker` nacks it:

```python linenums="1"
{! docs_src/redis/stream/pel_reprocessing.py !}
```

```python linenums="1"
{! docs_src/redis/stream/pel_testing.py [ln:54-60] !}
```

!!! tip
    Unlike a real broker, the fake `min_idle_time` consumer doesn't wait for the idle timeout to elapse - it checks the PEL immediately, so the reclaim happens within the same `publish()` call that produced the pending entry.

## `no_ack` Skips the PEL Entirely

Because `#!python no_ack=True` disables acknowledgement altogether, the fake broker never records an entry for it, even when the handler raises:

```python linenums="1"
{! docs_src/redis/stream/pel_no_ack.py !}
```

```python linenums="1"
{! docs_src/redis/stream/pel_testing.py [ln:43-51] !}
```

## Inspecting the `PEL`

By default, each `TestRedisBroker` creates its own private `PEL`. Passing one explicitly (as in the examples above) lets you assert against it directly - either by reading `pel._entries`, or through the `put`/`remove` spies it exposes:

```python
from unittest.mock import patch

from faststream.redis.testing import PEL, TestRedisBroker

pel = PEL()

async with TestRedisBroker(broker, pel=pel) as br:
    with patch.object(pel, "put") as put_mock:
        await br.publish(...)

    put_mock.assert_not_called()
```
