---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Custom Serialization

By default, **FastStream** uses the *JSON* format to send and receive messages. However, if you need to handle messages in other formats or with additional serialization steps, such as *gzip*, *lz4*, *Avro*, *Protobuf* or *Msgpack*, you can easily modify the serialization logic.

## Serialization Steps

Before the message reaches your subscriber, **FastStream** applies two functions to it sequentially: `parse_message` and `decode_message`. You can modify one or both stages depending on your needs.

### Message Parsing

At this stage, **FastStream** serializes an incoming message from the broker's framework into a general format called - **StreamMessage**. During this stage, the message body remains in the form of raw bytes.

!!! warning ""
    This stage is closely related to the features of the used broker, and in most cases, redefining it is not necessary. Redefine it, only if you know what you are doing.

The parser declared at the `broker` level will be applied to all subscribers. The parser declared at the `subscriber` level is applied only to that specific subscriber and overrides the `broker`'s parser if specified.

### Message Decoding

At this stage, the body of the **StreamMessage** is transformed into a format suitable for processing within your subscriber function. This is the stage you may need to redefine more often.
