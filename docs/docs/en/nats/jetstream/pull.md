---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Pull Subscriber

## Overview

**NATS JetStream** supports two different ways to consume messages: [**Push** and **Pull**](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#push-and-pull-consumers){.external-link target="_blank"} consumers.

The **Push** consumer is used by default to consume messages with **FastStream**. It means that the **NATS** server delivers messages to your consumer as fast as possible by itself. However, it also means that **NATS** should control all current consumer connections and increase server load.

Thus, the **Pull** consumer is the recommended way to consume JetStream messages by the *NATS TEAM*. Using it, you simply ask **NATS** for new messages at some interval. It may sound a little less convenient than automatic message delivery, but it provides several advantages, such as:

* Consumer scaling without a *queue group*
* Handling messages in batches
* Reducing **NATS** server load

So, if you want to consume a large flow of messages without strict time limitations, the **Pull** consumer is the right choice for you.

## FastStream Details

The **Pull** consumer is just a regular *Stream* consumer, but with the `pull_sub` argument, which controls consuming messages with batch size and block interval.

```python linenums="1" hl_lines="10-11"
{! docs_src/nats/js/pull_sub.py !}
```

The batch size doesn't mean that your `msg` argument is a list of messages, but it means that you consume up to `#!python 10` messages for one request to **NATS** and call your handler for each message concurrently in an `anyio` task group.

!!! tip
    If you want to consume a list of messages, just set the `batch=True` in `PullSub` class.

### Batch Pull Subscriber Example

This example demonstrates how to use `batch=True` with a Pull subscriber.
The message bodies are passed as a list into `bodies`, while the native NATS
messages are available via `msg.raw_message` (type `NatsBatchMessage`),
allowing per‑message `ack()` inside a batch.

```python linenums="1" hl_lines="10-11"
{! docs_src/nats/js/pull_sub_batch_example.py !}
```

So, your subject will be processed much faster, without blocking for each message processing.

`batch_size` is an upper bound, not a target: a batch is never filled up to it by waiting. Each request takes the messages already stored in the stream, up to `batch_size`, and returns at once. If the stream holds `#!python 3` messages, the handler gets a batch of `#!python 3`.

`timeout` (5 seconds by default) applies only when the stream is empty: the request waits up to that long for new messages, and returns as soon as the first one arrives. So when messages are published one by one and consumed as fast as they come, every batch has a single message.

!!! tip
    To receive fuller batches, publish in bursts, or let messages build up in the stream before they are consumed.
