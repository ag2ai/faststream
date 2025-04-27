---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publishing Basics

**FastStream** is broker-agnostic and easy to use, even as a client in non-**FastStream** applications.

It offers several use cases for publishing messages:

* Using `#!python broker.publish(...)` method
* Using the `#!python @broker.publisher(...)` decorator
* Using a publisher object decorator
* Using a publisher object directly

All of these variants have their own advantages and limitations, so you can choose what you want based on your requirements. Please visit the following pages for details.

## Serialization

**FastStream** allows you to publish any JSON-serializable messages (Python types, Pydantic models, etc.) or raw bytes.

It automatically sets up all required headers, especially the `correlation_id`, which is used to trace message processing pipelines across all services.

The `content-type` is a meaningful header for **FastStream** services. It helps the framework serialize messages faster, selecting the right serializer based on the header. This header is automatically set by **FastStream** too, but you should set it up manually using other libraries to interact with **FastStream** applications.

Content-Type can be:

* `text/plain`
* `application/json`
* empty with bytes content

By the way, you can use `application/json` for all of your messages if they are not raw bytes. You can even omit using any header at all, but it makes serialization slightly slower.

## Publishing

**FastStream** can also be used as a Broker client to send messages in other applications. It is quite straightforward and similar to *aiohttp* or *requests*.

You just need to `#!python connect` your broker, and you are ready to send a message. Additionally, you can use *Broker* as an async context manager to establish a connection and disconnect when leaving the scope.

To publish a message, simply set up the message content and a routing key:

=== "AIOKafka"
    ```python
    async with KafkaBroker() as br:
        await br.publish("message", "topic")
    ```

=== "Confluent"
    ```python
    async with KafkaBroker() as br:
        await br.publish("message", "topic")
    ```

=== "RabbitMQ"
    ```python
    async with RabbitBroker() as br:
        await br.publish("message", "queue")
    ```

=== "NATS"
    ```python
    async with NatsBroker() as br:
        await br.publish("message", "subject")
    ```

=== "Redis"
    ```python
    async with RedisBroker() as br:
        await br.publish("message", "channel")
    ```
