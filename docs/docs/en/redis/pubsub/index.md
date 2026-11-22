---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
title: "Redis Pub/Sub in Python: Async Publish and Subscribe"
description: >-
  Publish and subscribe to Redis channels in async Python with redis-py and FastStream. Run a channel example and learn when to use Streams for durable processing.
search:
  boost: 10
---

# Redis Pub/Sub in Python: Async Publish and Subscribe

Use **Redis Pub/Sub in Python** to broadcast messages to applications listening on a channel. FastStream uses **redis-py** for Redis access and lets you declare asynchronous subscribers and publishers with decorators. Pub/Sub delivers to currently connected subscribers; use [Redis Streams](../streams/index.md) when you need stored messages, consumer groups or acknowledgement.

## Run an async Python publisher and subscriber

Install the Redis backend and CLI:

```bash
pip install "faststream[redis,cli]"
```

With Redis listening on `localhost:6379`, save this application as `redis_pubsub.py`:

```python linenums="1"
{! docs_src/index/redis/basic.py !}
```

Start the application:

```bash
faststream run redis_pubsub:app
```

In a second terminal, listen for the response:

```bash
redis-cli SUBSCRIBE out-channel
```

In a third terminal, publish JSON matching the Python handler's arguments:

```bash
redis-cli PUBLISH in-channel '{"user": "Alice", "user_id": 1}'
```

The subscriber processes the message and the publisher sends its return value, `User: 1 - Alice registered`, to `out-channel`. FastStream wraps outgoing messages in its [binary message format](../message_format.md#publishing-to-an-external-consumer), so `redis-cli` shows envelope bytes containing this response rather than plain text alone. Another FastStream subscriber decodes the body automatically. Keep the subscriber connected before publishing: Pub/Sub does not replay earlier messages.

For publishing directly from Python, see [channel publishing](publishing.md). For wildcard channels such as `logs.*`, see [pattern subscriptions](subscription.md#pattern-channel-subscription).

!!! tip "Cluster Support"
    `RedisClusterBroker` supports Pub/Sub via a synchronous `RedisCluster` client. See the [Cluster docs](../cluster.md){.internal-link}.

## Delivery and limitations

[Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/){.external-link target="_blank"} broadcasts each published message to the subscribers currently connected to its channel. It suits live notifications and updates when missing a message during a disconnection is acceptable.

- **No replay or persistence**: channel messages are not retained for subscribers that connect later.
- **No processing acknowledgement**: Redis Pub/Sub uses at-most-once delivery. A subscriber that disconnects or fails to process a received message cannot ask Pub/Sub to redeliver it.
- **Delivery order and processing order differ**: Redis delivers Pub/Sub messages in publication order, but concurrent handler execution can complete out of order.

Choose [Redis Streams](../streams/index.md) for retained entries, consumer groups, acknowledgement and recovery of pending messages. Choose [Redis Lists](../list/index.md) for a queue based on list operations.
