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
Your code buffers them in memory and flushes when the batch is full or when a
timeout elapses.

!!! warning
    This pattern keeps **unacknowledged** messages in process memory until a batch completes. Processing and acknowledgement are **not atomic**: if the worker stops after your business logic but before every `ack`, RabbitMQ will redeliver those messages. **Batch handling must be idempotent** — safe to run more than once for the same payload.

!!! note
    The collector lives in a single worker process. Running multiple app instances does **not** build one shared batch — each instance buffers its own deliveries.

## Batch Subscriber Pattern

Let's dive into example code:

```python linenums="1" hl_lines="11 12"
{!> docs_src/rabbit/subscription/batch.py [ln:1-12] !}
```

Here we start with the imports, broker setup, and batch settings.

`BATCH_SIZE` is the maximum number of messages per batch. `FLUSH_INTERVAL` is the timeout (in seconds) used when the batch is not full yet.
We keep the logic in a small `BatchCollector` class so the shared state (buffers, lock, and timer task) stays explicit:

```python linenums="1" hl_lines="5 6 7 8"
{!> docs_src/rabbit/subscription/batch.py [ln:15-22] !}
```

In the constructor:

- `self.data` — decoded message payloads
- `self.msg` — raw `RabbitMessage` objects (needed for manual `ack` / `nack`)
- `self.lock` — protects the buffers from concurrent access (`add` vs timeout flush)
- `self._timer` — holds the timeout task so it can be cancelled when the batch fills

The main entry point is `add()`:

```python linenums="1" hl_lines="10 11 12 14 15"
{!> docs_src/rabbit/subscription/batch.py [ln:25-39] !}
```

`add()` appends the current message under the lock. If it is the first message in a new batch, it starts the timeout task. When the batch reaches `BATCH_SIZE`, it cancels the timer, **detaches** the current buffers via `_take_batch()`, and releases the lock. Only then does it call `_flush()` on the detached batch — so concurrent deliveries cannot grow a batch past `BATCH_SIZE`.

```python linenums="1"
{!> docs_src/rabbit/subscription/batch.py [ln:41-54] !}
```

- `_take_batch()` — copies and clears the shared buffers while the lock is held
- `_cancel_timer()` — cancels the pending timeout task (but never cancels itself)

```python linenums="1" hl_lines="8 9 10 11"
{!> docs_src/rabbit/subscription/batch.py [ln:56-71] !}
```

`_on_timeout()` waits for `FLUSH_INTERVAL`, then uses the same detach-and-flush flow for a partial batch.

```python linenums="1" hl_lines="14 15 16 17 18"
{!> docs_src/rabbit/subscription/batch.py [ln:73-94] !}
```

`_flush()` receives an already detached batch, runs your batch work (here: a short `sleep` as a stand-in for a bulk DB write), then acknowledges every message. On failure it `nack`s the whole batch.

Wire the collector into **FastStream**: create a `BatchCollector`, subscribe with `AckPolicy.MANUAL`, and forward each message into `collector.add()`.

```python linenums="1" hl_lines="4 5 6"
{!> docs_src/rabbit/subscription/batch.py [ln:97-113] !}
```

!!! note
    Use `AckPolicy.MANUAL` and limit in-flight messages with `Channel(prefetch_count=...)` on `@broker.subscriber`. Prefetch is your main defense against unbounded memory growth while messages wait for a full batch or a timeout.

??? example "Full Example"
    ```python linenums="1"
    {!> docs_src/rabbit/subscription/batch.py !}
    ```
