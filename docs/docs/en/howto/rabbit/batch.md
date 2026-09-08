---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Rabbit Batch Subscriber

**FastStream** does not provide a built-in batch consumer API for **RabbitMQ**. Hiding message accumulation behind a special subscriber option would obscure how prefetch, manual acknowledgement, and buffering actually work — so this page shows an **application-level** pattern instead.

In the example below, **FastStream** still delivers messages **one by one**.
Your code buffers them in memory and flushes when the batch reaches `BATCH_SIZE`. For sparse traffic, add a timeout (or shutdown drain) so a partial batch does not sit unacked forever.

!!! warning
    This pattern keeps **unacknowledged** messages in process memory until a batch completes. Processing and acknowledgement are **not atomic**: if the worker stops after your business logic but before every `ack`, RabbitMQ will redeliver those messages. **Batch handling must be idempotent** — safe to run more than once for the same payload.

!!! note
    The buffer lives in a single worker process. Running multiple app instances does **not** build one shared batch — each instance buffers its own deliveries.

## Batch Subscriber Pattern

Let's dive into example code:

```python linenums="1" hl_lines="8 9 10"
{!> docs_src/rabbit/subscription/batch.py [ln:1-10] !}
```

Broker setup, `BATCH_SIZE`, and shared state: an `asyncio.Lock`, the in-memory `batch` buffer.

```python linenums="1"
{!> docs_src/rabbit/subscription/batch.py [ln:13-27] !}
```

- `take_batch()` — **detaches** the current buffer under the lock (`copy` + `clear`)
- `flush()` — runs your batch work on the detached items, then acknowledges each message

```python linenums="1" hl_lines="8 9 10 11 12 13"
{!> docs_src/rabbit/subscription/batch.py [ln:29-43] !}
```

Under the lock the handler appends the message, and when the buffer reaches `BATCH_SIZE` it calls `take_batch()`. After releasing the lock it calls `flush()` on that detached batch — so concurrent deliveries cannot grow a batch past `BATCH_SIZE`.

!!! note
    Use `AckPolicy.MANUAL` and limit in-flight messages with `Channel(prefetch_count=...)` on `@broker.subscriber`. Prefetch is your main defense against unbounded memory growth while messages wait for a full batch.

??? example "Full Example"
    ```python linenums="1"
    {!> docs_src/rabbit/subscription/batch.py !}
    ```
