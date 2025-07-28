---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Context Fields Declaration

You can also store your own objects in the `Context`.

## Global

To declare an application-level context field, you need to call the `context.set_global` method with a key to indicate where the object will be placed in the context.

=== "AIOKafka"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/kafka/custom_global_context.py [ln:1-5,13-16] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/confluent/custom_global_context.py [ln:1-5,13-16] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/rabbit/custom_global_context.py [ln:1-5,13-16] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/nats/custom_global_context.py [ln:1-5,13-16] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/redis/custom_global_context.py [ln:1-5,13-16] !}
    ```

Afterward, you can access your `secret` field in the usual way:

=== "AIOKafka"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/kafka/custom_global_context.py [ln:8-13] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/confluent/custom_global_context.py [ln:8-13] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/rabbit/custom_global_context.py [ln:8-13] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/nats/custom_global_context.py [ln:8-13] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3"
    {!> docs_src/getting_started/context/redis/custom_global_context.py [ln:8-13] !}
    ```

In this case, the field becomes a global context field: it does not depend on the current message handler (unlike `message`)

!!! tip
    Alternatively you can setup gloval context objects in `FastStream` object constructor:

    ```python
    from faststream import FastStream, ContextRepo

    app = FastStream(context=ContextRepo({
        "secret_str": "my-perfect-secret"
    }))
    ```

To remove a field from the context use the `reset_global` method:

```python
context.reset_global("my_key")
```

## Local

To set a local context (available only within the message processing scope), use the context manager `scope`. It could me extremely uselful to fill context with additional options in [Middlewares](../middlewares/){.internal-link}

=== "AIOKafka"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/kafka/custom_local_context.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/confluent/custom_local_context.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/rabbit/custom_local_context.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/nats/custom_local_context.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="13 22"
    {!> docs_src/getting_started/context/redis/custom_local_context.py !}
    ```
