---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Tutorial

## Installation

=== "PostgreSQL"
    ```bash
    pip install "faststream[sqla-postgres]"
    ```

=== "MySQL"
    ```bash
    pip install "faststream[sqla-mysql]"
    ```

## Database Tables

The SQLA broker requires two tables — `message` (active messages) and `message_archive` (completed/failed messages). You can customize them to your liking (partition them, add indices, specify more specific data types, etc.) as long as they generally conform to the following schemas. Schema check is done on startup if `SqlaBroker`'s `validate_schema_on_start` is `True`.

```python linenums="1"
{!> docs_src/sqla/tables.py !}
```

## Broker

```python
from sqlalchemy.ext.asyncio import create_async_engine

from faststream.sqla import SqlaBroker

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlaBroker(engine=engine)
```

#### Broker parameters

- **`engine`** — SQLAlchemy `AsyncEngine` to use for requests to the database.
- **`validate_schema_on_start`** — If `True` (default), validates that the `message` and `message_archive` tables exist and conform to the expected schema.
- **`graceful_timeout`** — Seconds to wait for in-flight messages to finish processing during shutdown.

## Publishing

```python linenums="1"
{!> docs_src/sqla/publish.py [ln:1-16]!}
```

The broker's and publisher's (see [publishing](../getting-started/publishing/index.md){.internal-link}) `.publish()` methods accept:

- **`message`** — The message body.
- **`queue`** — The target queue name.
- **`headers`** — Optional `dict[str, str]` of message headers.
- **`next_attempt_at`** — Optional `datetime` (with timezone) for delayed delivery.
- **`connection`** — Optional SQLAlchemy `AsyncConnection` for transactional publishing.

### Delayed delivery

The message won't be fetched until `next_attempt_at` if it is provided.
```python linenums="1"
{!> docs_src/sqla/publish.py [ln:18-22]!}
```

### Transactional publishing

When `connection` is provided, the message insert participates in the same database transaction as your other operations, enabling the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html){.external-link target="_blank"}.
```python linenums="1"
{!> docs_src/sqla/publish.py [ln:24-30]!}
```

## Subscribing

```python linenums="1"
{!> docs_src/sqla/subscribe.py !}
```

#### Subscriber parameters

- **`queues`** — List of queue names to consume from.
- **`max_workers`** — Number of workers to process messages concurrently.
- **`retry_strategy`** — Called to determine if and when a Nack'ed message is retried. If `None`, `AckPolicy.NACK_ON_ERROR` has the same effect as `AckPolicy.REJECT_ON_ERROR`.
- **`min_fetch_interval`** — Minimum interval between consecutive fetches, used if the last fetch was full (returned as many messages as the fetch's limit).
- **`max_fetch_interval`** — Maximum interval between consecutive fetches.
- **`fetch_batch_size`** — Maximum number of messages to fetch in a single batch. A fetch's actual limit might be lower if the free capacity of the acquired-but-not-yet-in-processing buffer is smaller.
- **`overfetch_factor`** — Multiplier for `fetch_batch_size` to size the internal buffer of acquired-but-not-yet-processing messages.
- **`flush_interval`** — Interval between flushes of processed message state to the database.
- **`release_stuck_interval`** — Interval between checks for stuck [`PROCESSING`](../sqla/design.md#message-lifecycle){.internal-link} messages.
- **`release_stuck_timeout`** — Interval since `acquired_at` after which a [`PROCESSING`](../sqla/design.md#message-lifecycle){.internal-link} message is considered stuck and is released back to [`PENDING`](../sqla/design.md#message-lifecycle){.internal-link}.
- **`max_deliveries`** — Maximum number of deliveries allowed for a message. If set, messages that have reached this limit are Reject'ed to [`FAILED`](../sqla/design.md#message-lifecycle){.internal-link} without processing. Note that this might violate the [at-least-once](../sqla/design.md#poison-message-protection){.internal-link} processing semantics.
- **`ack_policy`** — [AckPolicy](../getting-started/acknowledgement.md){.internal-link} that controls acknowledgement behavior.

### Delayed retries

When a message is Nack'ed (either manually or by `AckPolicy.NACK_ON_ERROR`), the `retry_strategy` determines if and when the message should be retried. All strategies accept `max_attempts` and `max_total_delay_seconds` as common parameters — if either limit is reached, the message is marked as [`FAILED`](../sqla/design.md#message-lifecycle){.internal-link} instead of [`RETRYABLE`](../sqla/design.md#message-lifecycle){.internal-link}. Otherwise, the strategy schedules the message for a retry determined by the returned `next_attempt_at`.

#### `ConstantRetryStrategy`

Retries after a fixed `delay_seconds` every time.

```python linenums="1"
{!> docs_src/sqla/retry.py [ln:10-14]!}
```

#### `LinearRetryStrategy`

First retry after `initial_delay_seconds`, then the delay increases by `step_seconds` with each attempt.

```python linenums="1"
{!> docs_src/sqla/retry.py [ln:16-21]!}
```

#### `ExponentialBackoffRetryStrategy`

Delay starts at `initial_delay_seconds` and is multiplied by `multiplier` on each attempt. `max_delay_seconds` caps the delay.

```python linenums="1"
{!> docs_src/sqla/retry.py [ln:23-29]!}
```

#### `ExponentialBackoffWithJitterRetryStrategy`

Same as exponential backoff, but adds random jitter (up to `delay * jitter_factor`) to spread out retries and avoid thundering herds.

```python linenums="1"
{!> docs_src/sqla/retry.py [ln:31-38]!}
```

#### `ConstantWithJitterRetryStrategy`

Retries after `base_delay_seconds` plus random jitter in the range `[-jitter_seconds, +jitter_seconds]`.

```python linenums="1"
{!> docs_src/sqla/retry.py [ln:40-45]!}
```

#### `NoRetryStrategy`

No retries — the message is marked as [`FAILED`](../sqla/design.md#message-lifecycle){.internal-link} on the first Nack.

```python linenums="1"
{!> docs_src/sqla/retry.py [ln:47]!}
```
