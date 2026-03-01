---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Basic Subscriber

If you know nothing about *RabbitMQ* and how it works, you will still able to use **FastStream RabbitBroker**.

Just use the `#!python @broker.subscriber(...)` method with a string as a routing key.

```python linenums="1"
{! docs_src/rabbit/subscription/index.py !}
```

This is the principle all **FastStream** brokers work by: you don't need to learn them in-depth if you want to *just send a message*.

## RabbitMQ Details

If you are already familiar with *RabbitMQ* logic, you should also be acquainted with the inner workings of the example mentioned above.

In this case, **FastStream** either creates or validates a queue with a specified **routing_key** and binds it to the default *RabbitMQ* exchange.

If you want to specify a *queue*-*exchange* pair with additional arguments, **FastStream** provides you with the ability to do so. You can use special `RabbitQueue` and `RabbitExchange` objects to configure RabbitMQ queues, exchanges, and binding properties. For examples of using various types of exchanges, please refer to the following articles.
