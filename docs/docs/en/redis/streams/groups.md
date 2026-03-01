---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis Stream Consumer Groups

Consuming messages from a **Redis** stream can be accomplished by using a Consumer Group. This allows multiple consumers to divide the workload of processing messages in a stream and provides a form of message acknowledgment, ensuring that messages are not processed repeatedly.

Consumer Groups in Redis enable a group of clients to cooperatively consume different portions of the same stream of messages. When using `#!python group="..."` (which internally uses `XREADGROUP`), messages are distributed among different consumers in a group and are not delivered to any other consumer in that group again, unless they are not acknowledged (i.e., the client fails to process and does not call `msg.ack()` or `XACK`). This is in contrast to a normal consumer (also known as `XREAD`), where every consumer sees all the messages. `XREAD` is useful for broadcasting to multiple consumers, while `XREADGROUP` is better suited for workload distribution.

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

  If you don't want to collect data into stream forever, you should using the **maxlen** option. The old entries are automatically evicted when the specified length is reached, so that the stream is left at a consist size. [Redis maxlen](https://redis.io/docs/latest/develop/data-types/streams/#capped-streams){.external-link target="_blank"}

  A consumer group consists multiple consumer instances working together. You need giving each a unique name using the **consumer** option. [Redis Consumers](https://redis.io/docs/latest/develop/tools/insight/tutorials/insight-stream-consumer/#run-the-consumer){.external-link target="_blank"}

  In cases where reliability is not a requirement and the occasional message loss is acceptable, you can use **no_ack** option. This is equivalent to acknowledging the message when it is read. [Redis Xgroupread](https://redis.io/docs/latest/commands/xreadgroup/#differences-between-xread-and-xreadgroup){.external-link target="_blank"}
