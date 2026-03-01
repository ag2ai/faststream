---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Rabbit Routing

!!! note ""
      **FastStream** *RabbitMQ* support is implemented on top of [**aio-pika**](https://docs.aio-pika.com/){.external-link target="_blank"}. You can always get access to objects of it, if you need to use some low-level methods, not represented in **FastStream**.

## Advantages

The advantage of *RabbitMQ* is the ability to configure flexible and complex message routing scenarios.

*RabbitMQ* covers the whole range of routing: from one queue - one consumer, to a queue retrieved from several sources, including message prioritization.

!!! note
      For more information about *RabbitMQ*, please visit the [official documentation](https://www.rabbitmq.com/tutorials/amqp-concepts.html){.external-link target="_blank"}

It supports the ability to successfully process messages, mark them as processed with an error, remove them from the queue (it is also impossible to re-receive processed messages, unlike **Kafka**), lock it for the processing duration, and monitor its current status.

Having to keep track of the current status of all messages is a cause of the *RabbitMQ* performance issues. With really large message volumes, *RabbitMQ* starts to degrade. However, if this was a "one-time influx", then consumers will free the queue of messages and the "health" of *RabbitMQ* will be stable.

If your scenario is not based on processing millions of messages and also requires building complex routing logic, *RabbitMQ* will be the right choice.

## Basic Concepts

If you want to totally understand how *RabbitMQ* works, you should visit their official website. There you will find top-level comments about the basic concepts and usage examples.

### Entities

*RabbitMQ* works with three main entities:

* `Exchange` - the point of receiving messages from *publisher*
* `Queue` - the point of pushing messages to *consumer*
* `Binding` - the relationship between *queue-exchange* or *exchange-exchange*

### Routing Rules

The rules for delivering messages to consumers depend on the **type of exchange** and **binding** parameters. All the main options will be discussed at [examples](examples/direct.md){.internal-link}.

In general, the message path looks so:

1. *Publisher* sends a message to `exchange`, specify its `routing_key` and headers according to which routing will take place.
2. `Exchange`, depending on the message parameters, determines which of the subscribed `bindings` to send the message to.
3. `Binding` delivers the message to `queue` or another `exchange` (in this case it will send it further by its own rules).
4. `Queue`, after receiving a message, sends it to one of subscribed consumers (**PUSH API**).

!!! tip
    By default, all queues have a `binding` to the `default exchange` (**Direct** type) with a **routing key** corresponding to their name.
    In **FastStream**, queues are connected to this `exchange`, and messages are sent by default unless another `exchange` is explicitly specified.

    !!! warning ""
        Connecting the queue to any other `exchange` will still leave it subscribed to the `default exchange'. Be careful with this.

At this stage, the message gets into your application - and you start processing it.

### Message Statuses

*RabbitMQ* requires confirmation of message processing: only after that, it will be removed from the queue.

Confirmation can be either positive (`Acknowledgment - ack`) if the message was successfully processed or negative (`Negative Acknowledgment - nack`) if the message was processed with an error.

At the same time, in case of an error, the message can also be extracted from the queue (`reject`); otherwise, after a negative confirmation, it will be requeued for processing again.

In most cases, **FastStream** performs all the necessary actions by itself. However, if you want to manage the message lifecycle directly, you can access the message object itself and call the appropriate methods directly. This can be useful if you want to implement an "at most once" policy and you need to confirm the consuming of the message before it is actually processed.

## **FastStream** Specific

**FastStream** omits the ability to create `bindings` directly, since in most cases, you do not need to subscribe one queue to several `exchanges` or subscribe `exchanges` to each other. On the contrary, this practice leads to over-complication of the message routing scheme, which makes it difficult to maintain and further develop the entire infrastructure of services.

**FastStream** suggests you adhere to the scheme `exchange:queue` as `1:N`, which will greatly simplify the scheme of interaction between your services. It is better to create an additional queue for a new `exchange` than to subscribe to an existing one.

However, if you want to reduce the number of entities in your *RabbitMQ*, and thereby optimize its performance (or you know exactly what you are doing), **FastStream** leaves you the option to create `bindings` directly. In other cases, the connection parameters are an integral part of the entities **RabbitQueue** and **RabbitExchange** in **FastStream**.
