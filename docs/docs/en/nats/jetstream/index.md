---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# NATS JetStream

The default *NATS* usage is suitable for scenarios where:

* The `publisher` and `consumer` are always online.
* The system can tolerate messages loss.

If you need stricter restrictions, like:

* An availability of a message processing confirmation mechanism (`ack`/`nack`).
* Message persistence (messages will accumulate in the queue when the `consumer` is offline).

You should use the **NATS JetStream** extension.

In fact, the **JetStream** extension is the same as *NATS*, with the addition of a persistent layer above the file system. Therefore, all interfaces for publishing and consuming messages are similar to regular *NATS* usage.

However, the **JetStream** layer has many possibilities for configuration, from the policy of deleting old messages to the maximum stored messages number limit. You can find out more about all **JetStream** features in the official [documentation](https://docs.nats.io/using-nats/developer/develop_jetstream){.external-link target="_blank"}.

!!! tip ""
    If you have worked with other message brokers, then you should know that the logic of **JS** is closer to **Kafka** than to **RabbitMQ**: messages, after confirmation, are not deleted from the queue but remain there until the queue is full, and it will start deleting old messages (or in accordance with other logic that you can configure yourself).

    When connecting a `consumer` (and, especially, when reconnecting), you must determine for yourself according to what logic it will consume messages: from the subject beginning, starting with some message, starting from some time, only new ones, etc. Don't be surprised if a connection is restored, and your `consumer` starts to process all messages received earlier again - you haven't defined the rule.

Also, **NATS JetStream** has built-in `key-value` (similar to **Redis**) and `object` (similar to **Minio**) storages, which, in addition to the interface for *put/get*, have the ability to subscribe to events, which can be extremely useful in various scenarios.

**FastStream** does not provide access to this functionality directly, but it is covered by the [nats-py](https://github.com/nats-io/nats.py){.external-link target="_blank"} library used. You can access the **JS** object from the application context:

```python linenums="1" hl_lines="2 7 11-12 21"
{! docs_src/nats/js/main.py !}
```

!!! tip
    Using `JStream` object **FastStream** is trying to create/update stream with the object settings. To prevent this behavior and *just get already created stream*, please use `#!python JStream(..., declare=False)` option.
