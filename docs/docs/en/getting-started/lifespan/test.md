---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Events Testing

In the most cases you are testing your subscriber/publisher functions, but sometimes you need to trigger some lifespan hooks in your tests too.

For this reason, **FastStream** has a special **TestApp** patcher working as a regular async context manager.

=== "AIOKafka"
    ```python linenums="1" hl_lines="3 18"
    {!> docs_src/getting_started/lifespan/kafka/testing.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3 18"
    {!> docs_src/getting_started/lifespan/confluent/testing.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3 18"
    {!> docs_src/getting_started/lifespan/rabbit/testing.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3 18"
    {!> docs_src/getting_started/lifespan/nats/testing.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3 18"
    {!> docs_src/getting_started/lifespan/redis/testing.py !}
    ```

## Using with **TestBroker**

If you want to use In-Memory patched broker in your tests, it's advised to patch the broker first (before applying the application patch).

Also, **TestApp** and **TestBroker** are both calling `#!python broker.start()`. According to the original logic, broker should be started in the `FastStream` application, but if **TestBroker** is applied first – it breaks this behavior. For this reason, **TestApp** prevents **TestBroker** `#!python broker.start()` call if it is placed inside the **TestBroker** context.

This behavior is controlled by `connect_only` argument to **TestBroker**. While `None` is the default value, **TestApp** can set it to `True` or `False` during the code execution. If `connect_only` argument is provided manually, it would not be changed.

!!! warning
    With `#!python connect_only=False`, all `FastStream` hooks will be called after the **broker start**, which can break some `#!python @app.on_startup` logic.
