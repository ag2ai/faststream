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

**NATS JetStream** supports two different ways to consume messages: [**Push** and **Pull**](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#push-and-pull-consumers){.external-link target="_blank} consumers.

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

The batch size doesn't mean that your `msg` argument is a list of messages, but it means that you consume up to `#!python 10` messages for one request to **NATS** and call your handler for each message in an `asyncio.gather` pool.

!!! tip
    If you want to consume list of messages, just set the `batch=True` in `PullSub` class.

So, your subject will be processed much faster, without blocking for each message processing. However, if your subject has fewer than `#!python 10` messages, your request to **NATS** will be blocked for `timeout` (5 seconds by default) while trying to collect the required number of messages. Therefore, you should choose `batch_size` and `timeout` accurately to optimize your consumer efficiency.
