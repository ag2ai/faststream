---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
title: "Redis Stream Consumer Groups in Python"
description: >-
  Distribute Redis Stream messages across Python workers with XREADGROUP and FastStream. Configure group and consumer names, acknowledge messages and recover pending work.
search:
  boost: 10
---

# Redis Stream Consumer Groups in Python

A **Redis Stream consumer group** distributes messages across Python workers instead of broadcasting each entry to every subscriber. FastStream uses **redis-py** and `XREADGROUP` when you configure a `StreamSub` with `group` and `consumer`. Each worker in the group needs a unique consumer name; separate groups can independently process the same stream.

`XREADGROUP` assigns new entries to consumers in the same group and tracks entries awaiting acknowledgement in the Pending Entries List (PEL). `XACK` removes an entry from the group's PEL; it does not delete the entry from the stream. Without a group, `XREAD` lets independent subscribers read the same entries.

Acknowledgement does not make processing exactly once. A worker may complete a side effect and fail before acknowledging, leaving the entry available for [claiming and recovery](claiming.md). Make handlers idempotent when repeated processing would be harmful. Multiple workers can finish out of order even though stream entries have ordered IDs.

In the following example, we will create a simple FastStream app that utilizes a Redis stream with a Consumer Group. It will consume messages sent to the `#!python "test-stream"` as part of the `#!python "test-group"` consumer group.

The full app code is as follows:

```python linenums="1"
{! docs_src/redis/stream/group.py !}
```

## Import FastStream and RedisBroker

First, import the `FastStream` class and the `RedisBroker` from the `faststream.redis` module to define our broker.

```python linenums="1"
{! docs_src/redis/stream/group.py [ln:1-2] !}
```

## Create a RedisBroker

To establish a connection to Redis, instantiate a `RedisBroker` object and pass it to the `FastStream` app.

```python linenums="1"
{! docs_src/redis/stream/group.py [ln:4-5] !}
```

## Define a Consumer Group Subscription

Define a subscription to a Redis stream with a specific Consumer Group using the `StreamSub` object and the `#!python @broker.subscriber(...)` decorator. Then, define a function that will be triggered when new messages are sent to the `#!python "test-stream"` Redis stream. This function is decorated with `#!python @broker.subscriber(...)` and will process the messages as part of the `#!python "test-group"` consumer group.

```python linenums="1"
{! docs_src/redis/stream/group.py [ln:8-10] !}
```

## Publishing a message

Publishing a message is the same as what's defined on [Stream Publishing](./publishing.md).

```python linenums="1"
{! docs_src/redis/stream/group.py [ln:15.5] !}
```

By following the steps and code examples provided above, you can create a FastStream application that consumes messages from a Redis stream using a Consumer Group for distributed message processing.


## Redis Stream details

  If you don't want to collect data into stream forever, you should use the **maxlen** option. The old entries are automatically evicted when the specified length is reached, so that the stream is left at a consistent size. [Redis maxlen](https://redis.io/docs/latest/develop/data-types/streams/#capped-streams){.external-link target="_blank"}

  A consumer group consists of multiple consumer instances working together. You need to give each a unique name using the **consumer** option. [Redis Consumers](https://redis.io/docs/latest/develop/tools/insight/tutorials/insight-stream-consumer/#run-the-consumer){.external-link target="_blank"}

  In cases where reliability is not a requirement and the occasional message loss is acceptable, you can use the **no_ack** option. This is equivalent to acknowledging the message when it is read. [Redis Xgroupread](https://redis.io/docs/latest/commands/xreadgroup/#differences-between-xread-and-xreadgroup){.external-link target="_blank"}
