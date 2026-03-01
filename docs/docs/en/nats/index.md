---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# NATS

!!! note ""
      **FastStream** *NATS* support is implemented on top of [**nats-py**](https://github.com/nats-io/nats.py){.external-link target="_blank"}. You can always get access to objects of it if you need to use some low-level methods not represented in **FastStream**.

## Advantages and Disadvantages

*NATS* is an easy-to-use, high-performance message broker written in *Golang*. If your application does not require complex routing logic, can cope with high loads, scales, and does not require large hardware costs, *NATS* will be an excellent choice for you.

Also *NATS* has a zero-cost new entities creation (to be honest, all `subjects` are just routing fields), so it can be used as a **RPC** over **MQ** tool.

!!! note
    More information about *NATS* can be found on the [official website](https://nats.io){.external-link target="_blank"}.

However, *NATS* has disadvantages that you should be aware of:

* Messages are not persistent. If a message is published while your consumer is disconnected, it will be lost.
* There are no complex routing mechanisms.
* There are no mechanisms for confirming receipt and processing of messages from the consumer.

## NATS JetStream

These shortcomings are corrected by using the persistent level - [**JetStream**](https://docs.nats.io/nats-concepts/jetstream){.external-link target="_blank"}. If you need strict guarantees for the delivery and processing of messages at the small detriment of speed and resources consumed, you can use **NatsJS**.

Also, **NatsJS** supports some high-level features like *Key-Value* and *Object* storages (with subscription to changes on it) and provides you with rich abilities to build your logic on top of it.

## Routing Rules

*NATS* does not have the ability to configure complex routing rules. The only entity in *NATS* is `subject`, which can be subscribed to either directly by name or by a regular expression pattern.

Both examples are discussed [a little further](./examples/direct.md){.internal-link}.

In order to support the ability to scale consumers horizontally, *NATS* supports the `queue group` functionality:
a message sent to `subject` will be processed by a random consumer from the `queue group` subscribed to this `subject`.
This approach allows you to increase the processing speed of `subject` by *N* times when starting *N* consumers with one group.
