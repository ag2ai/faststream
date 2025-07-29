# Dynamic Subscribers

Sometimes, you need to process messages as they arrive. You may not know the source of the messages at startup. They could be sent to the service later: via an incoming message, request, or even generated randomly as a temporary queue for processing the response.
In these cases, you cannot use the regular **FastStream's** `#!python @broker.subscriber()` decorators.

However, the framework still allows you to do so in a suitable manner.

## Consuming a Single Message

To process a single message, you should create a subscriber and call the appropriate method on it. Don't forget to start the subscriber.

=== "AIOKafka"
    ```python linenums="1" hl_lines="8"
    {!> docs_src/getting_started/subscription/kafka/dynamic.py [ln:1-10] !}
    ```

    !!! note "Important"
        Do not forget to `start` and `stop` subscriber manually

        ```python linenums="1" hl_lines="1 5"
        {!> docs_src/getting_started/subscription/kafka/dynamic.py [ln:6-10] !}
        ```

=== "Confluent"
    ```python linenums="1" hl_lines="8"
    {!> docs_src/getting_started/subscription/confluent/dynamic.py [ln:1-10] !}
    ```

    !!! note "Important"
        Do not forget to `start` and `stop` subscriber manually

        ```python linenums="1" hl_lines="1 5"
        {!> docs_src/getting_started/subscription/confluent/dynamic.py [ln:6-10] !}
        ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="8"
    {!> docs_src/getting_started/subscription/rabbit/dynamic.py [ln:1-10] !}
    ```

    !!! note "Important"
        Do not forget to `start` and `stop` subscriber manually

        ```python linenums="1" hl_lines="1 5"
        {!> docs_src/getting_started/subscription/rabbit/dynamic.py [ln:6-10] !}
        ```

=== "NATS"
    ```python linenums="1" hl_lines="8"
    {!> docs_src/getting_started/subscription/nats/dynamic.py [ln:1-10] !}
    ```

    !!! note "Important"
        Do not forget to `start` and `stop` subscriber manually

        ```python linenums="1" hl_lines="1 5"
        {!> docs_src/getting_started/subscription/nats/dynamic.py [ln:6-10] !}
        ```

=== "Redis"
    ```python linenums="1" hl_lines="8"
    {!> docs_src/getting_started/subscription/redis/dynamic.py [ln:1-10] !}
    ```

    !!! note "Important"
        Do not forget to `start` and `stop` subscriber manually

        ```python linenums="1" hl_lines="1 5"
        {!> docs_src/getting_started/subscription/redis/dynamic.py [ln:6-10] !}
        ```

## Iteration over messages

However, if you want to process a stream of messages in a dynamic way, you should not use such unattractive methods as this one:

```python title="ugly_example.py" linenums="1"
while True:
    msg = await subscriber.get_one(timeout=3.0)
    if msg:
        ... # do message process
```

It would be much better to use the built-in iteration mechanism:

=== "AIOKafka"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/subscription/kafka/dynamic_iter.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/subscription/confluent/dynamic_iter.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/subscription/rabbit/dynamic_iter.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/subscription/nats/dynamic_iter.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/subscription/redis/dynamic_iter.py !}
    ```

!!! tip "Technical Details"
    Both ways support all **FastStream** features, such as  [middlewares](../../middlewares){.internal-link}, [OpenTelemetry tracing](../../observability/opentelemetry){.internal-link} and [Prometheus metrics](../../observability/prometheus){.internal-link}.


## Acknowledgement

Note that the default **FastStream** [acknowledgement](../../acknowledgement){.internal-link} logic does not work here. You will need to acknowledge a consumed message manually.

```python
msg = await subscriber.get_one()
await msg.ack()
```

And

```python
async for msg in subscriber:
    await msg.ack()
```
