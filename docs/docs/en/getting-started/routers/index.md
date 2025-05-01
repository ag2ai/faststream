---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Broker Router

Sometimes you want to:

* split an application into includable modules
* separate business logic from your handler registration
* apply some [decoder](../serialization/index.md)/[middleware](../middlewares/index.md)/[dependencies](../dependencies/global.md) to a subscribers group

For these reasons, **FastStream** has a special *Broker Router*.

## Router Usage

First, you need to import the *Broker Router* from the same module from where you imported the broker.

!!! note ""
    When creating a *Broker Router*, you can specify a prefix that will be automatically applied to all subscribers and publishers of this router.

=== "AIOKafka"
    ```python hl_lines="2 6 9-10 17 22"
    {!> docs_src/getting_started/routers/kafka/router.py [ln:1-6] !}
    ```

=== "Confluent"
    ```python hl_lines="2 6 9-10 17 22"
    {!> docs_src/getting_started/routers/confluent/router.py [ln:1-6] !}
    ```

=== "RabbitMQ"
    ```python hl_lines="2 6 9-10 17 22"
    {!> docs_src/getting_started/routers/rabbit/router.py [ln:1-6] !}
    ```

=== "NATS"
    ```python hl_lines="2 6 9-10 17 22"
    {!> docs_src/getting_started/routers/nats/router.py [ln:1-6] !}
    ```

=== "Redis"
    ```python hl_lines="2 6 9-10 17 22"
    {!> docs_src/getting_started/routers/redis/router.py [ln:1-6] !}
    ```

Now you can use the created router to register handlers and publishers as if it were a regular broker

=== "AIOKafka"
    ```python hl_lines="1-2 9"
    {!> docs_src/getting_started/routers/kafka/router.py [ln:9-19] !}
    ```

=== "Confluent"
    ```python hl_lines="1-2 9"
    {!> docs_src/getting_started/routers/confluent/router.py [ln:9-19] !}
    ```

=== "RabbitMQ"
    ```python hl_lines="1-2 9"
    {!> docs_src/getting_started/routers/rabbit/router.py [ln:9-19] !}
    ```

=== "NATS"
    ```python hl_lines="1-2 9"
    {!> docs_src/getting_started/routers/nats/router.py [ln:9-19] !}
    ```

=== "Redis"
    ```python hl_lines="1-2 9"
    {!> docs_src/getting_started/routers/redis/router.py [ln:9-19] !}
    ```

Then you can simply include all the handlers declared using the router in your broker


```python
{!> docs_src/getting_started/routers/kafka/router.py [ln:22] !}
```

Please note that when publishing a message, you now need to specify the same prefix that you used when creating the router

=== "AIOKafka"
    ```python hl_lines="3"
    {!> docs_src/getting_started/routers/kafka/router.py [ln:27.5,28.5,29.5,30.5] !}
    ```

=== "Confluent"
    ```python hl_lines="3"
    {!> docs_src/getting_started/routers/confluent/router.py [ln:27.5,28.5,29.5,30.5] !}
    ```

=== "RabbitMQ"
    ```python hl_lines="3"
    {!> docs_src/getting_started/routers/rabbit/router.py [ln:27.5,28.5,29.5,30.5] !}
    ```

=== "NATS"
    ```python hl_lines="3"
    {!> docs_src/getting_started/routers/nats/router.py [ln:27.5,28.5,29.5,30.5] !}
    ```

=== "Redis"
    ```python hl_lines="3"
    {!> docs_src/getting_started/routers/redis/router.py [ln:27.5,28.5,29.5,30.5] !}
    ```

!!! tip
    Also, when creating a *Broker Router*, you can specify [middleware](../middlewares/index.md), [dependencies](../dependencies/index.md#top-level-dependencies), [parser](../serialization/parser.md) and [decoder](../serialization/decoder.md) to apply them to all subscribers declared via this router.

## Delay Handler Registration

If you want to separate your application's core logic from **FastStream**'s routing logic, you can write some core functions and use them as *Broker Router* `handlers` later:

=== "AIOKafka"
    ```python linenums="1" hl_lines="9-17"
    {!> docs_src/getting_started/routers/kafka/router_delay.py [ln:3-4,9-13,15-25] !}
    ```

    Above example is identical to the following one:

    ```python linenums="1" hl_lines="1-2"
    {!> docs_src/getting_started/routers/kafka/delay_equal.py [ln:9-14] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="9-17"
    {!> docs_src/getting_started/routers/confluent/router_delay.py [ln:3-4,9-13,15-25] !}
    ```

    Above example is identical to the following one:

    ```python linenums="1" hl_lines="1-2"
    {!> docs_src/getting_started/routers/confluent/delay_equal.py [ln:9-14] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="9-17"
    {!> docs_src/getting_started/routers/rabbit/router_delay.py [ln:3-4,9-13,15-25] !}
    ```

    Above example is identical to the following one:

    ```python linenums="1" hl_lines="1-2"
    {!> docs_src/getting_started/routers/rabbit/delay_equal.py [ln:9-14] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="9-17"
    {!> docs_src/getting_started/routers/nats/router_delay.py [ln:3-4,9-13,15-25] !}
    ```

    Above example is identical to the following one:

    ```python linenums="1" hl_lines="1-2"
    {!> docs_src/getting_started/routers/nats/delay_equal.py [ln:9-14] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="9-17"
    {!> docs_src/getting_started/routers/redis/router_delay.py [ln:3-4,9-13,15-25] !}
    ```

    Above example is identical to the following one:

    ```python linenums="1" hl_lines="1-2"
    {!> docs_src/getting_started/routers/redis/delay_equal.py [ln:9-14] !}
    ```

!!! warning
    Be careful, this way you won't be able to test your handlers with a [`mock`](../subscription/test.md) object.
