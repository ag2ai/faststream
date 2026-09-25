---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# RabbitMQ Streams

*RabbitMQ* streams are persistent, replicated append-only logs. Unlike regular
queues, consuming a message does not remove it, so different consumers can read
the same messages or replay an earlier part of the log. Streams are useful for
large fan-outs, replay, high throughput, and large backlogs. See the
[RabbitMQ streams guide](https://www.rabbitmq.com/docs/streams){.external-link target="_blank"}
for the full feature and operational details.

## Declare and consume a stream

Set `queue_type=QueueType.STREAM` when declaring a `RabbitQueue`. Streams are
always durable, and RabbitMQ requires a non-zero consumer prefetch. The example
sets that prefetch on the broker's default channel.

```python linenums="1" hl_lines="4 10-14 18-20"
{! docs_src/rabbit/subscription/stream.py !}
```

## Choose the starting offset

Pass the RabbitMQ `x-stream-offset` consumer argument through `consume_args` to
choose where a subscriber starts reading. The example uses `first`, which
replays the stream from its first available message.

Supported values include:

* `first` to start at the first available message.
* `last` to start at the last stored chunk of messages, not only the final
  message.
* `next`, or no offset argument, to wait for messages published after the
  subscriber starts.
* An integer offset, a timestamp, or a relative time interval to start at a
  specific position.

## Configure retention

Because consumption does not delete messages, configure retention so a stream
does not grow without limit. The example keeps no more than seven days or 20 GB
of data by passing `x-max-age` and `x-max-length-bytes` in the queue's
`arguments` dictionary. You can also configure these limits with a RabbitMQ
policy. RabbitMQ removes the oldest stream segments when a retention limit is
reached.
