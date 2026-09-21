---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# RabbitMQ Queue/Exchange Declaration

## Declaring queues and exchanges

**FastStream** *subscribers* declares and validates all using *RabbitMQ* exchanges and queues (*publishers* declares exchanges only), but sometimes you need to declare them manually.

**RabbitBroker** provides a way to achieve this easily.

```python linenums="1" hl_lines="15-20 22-27"
{! docs_src/rabbit/declare.py !}
```

!!! note
    Both methods need an open connection, so call them after `#!python await broker.connect()` or, inside an application, in an `after_startup` hook as shown above. See [`connect()` and `start()`](./connection.md#connect-and-start){.internal-link} for the difference between the two entry points.

These methods require just one argument (`RabbitQueue`/`RabbitExchange`) containing information about your *RabbitMQ* required objects. They declare/validate *RabbitMQ* objects and return low-level **aio-pika** robust objects to interact with.

!!! tip
    Also, these methods are idempotent, so you can call them with the same arguments multiple times, but the objects will be created once; next time the method will return an already stored object. This way you can get access to any queue/exchange created automatically.


## Binding a queue to an exchange

It is also possible to bind a queue and an exchange using low-level **aio-pika** `RobustQueue.bind` method:

```python linenums="1" hl_lines="24-26 28-30 32-35"
{! docs_src/rabbit/bind.py !}
```
