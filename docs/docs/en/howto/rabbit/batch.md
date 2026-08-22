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

## Batch Subscriber Pattern

Let's dive into example code:

```python linenums="1" hl_lines="7 8 9 10 12 13"
{!> docs_src/rabbit/subscription/batch.py [ln:1-14] !}
```

Here we start with the imports, broker setup, and batch settings.

`BATCH_SIZE` is the maximum number of messages per batch. `FLUSH_INTERVAL` is the timeout (in seconds) used when the batch is not full yet.
We keep the logic in a small `BatchCollector` class so the shared state (buffers, lock, and timer task) stays explicit and we avoid a `global` timer variable:

```python linenums="1" hl_lines="5 6 7 8"
{!> docs_src/rabbit/subscription/batch.py [ln:15-22] !}
```

In the constructor:

- `self.data` — decoded message payloads
- `self.msg` — raw `RabbitMessage` objects (needed for manual `ack` / `nack`)
- `self.lock` — protects the buffers from concurrent access (`add` vs timeout flush)
- `self._timer` — holds the timeout task so it can be cancelled when the batch fills

The class exposes one main entry point and a few helpers.

```python linenums="1"
{!> docs_src/rabbit/subscription/batch.py [ln:26-39] !}
```

`add()` appends the current message. If it is the first message in a new batch, it starts the timeout task. If the batch reaches `BATCH_SIZE`, it cancels the timer and sets a flush flag. After releasing the lock, it calls `_flush()` when needed.

```python linenums="1"
{!> docs_src/rabbit/subscription/batch.py [ln:41-51] !}
```

Timer helpers:

- `_cancel_timer()` — cancels the pending timeout task
- `_on_timeout()` — waits for `FLUSH_INTERVAL`, then flushes the partial batch

```python linenums="1"
{!> docs_src/rabbit/subscription/batch.py [ln:53-75] !}
```

`_flush()` copies the current buffers into local lists, clears the collector state, runs your batch work (here: a short `sleep` as a stand-in for a bulk DB write), then acknowledges every message. On failure it `nack`s the whole batch.

```python linenums="1"
{!> docs_src/rabbit/subscription/batch.py [ln:78-91] !}
```

Wire the collector into **FastStream**: create a `BatchCollector`, subscribe with `AckPolicy.MANUAL`, and forward each message into `collector.add()`.

!!! note
    Use `AckPolicy.MANUAL` and limit in-flight messages with `Channel(prefetch_count=...)` on `@broker.subscriber`. Prefetch is your main defense against unbounded memory growth while messages wait for a full batch or a timeout.

??? example "Full Example"
    ```python linenums="1"
    {!> docs_src/rabbit/subscription/batch.py !}
    ```
