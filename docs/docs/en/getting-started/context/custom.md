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

To declare an application-level context field, you need to call the `context.set_global` method with with a key to indicate where the object will be placed in the context.

=== "AIOKafka"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/kafka/custom_global_context.py [ln:1-5,15-18] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/confluent/custom_global_context.py [ln:1-5,15-18] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/rabbit/custom_global_context.py [ln:1-5,15-18] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/nats/custom_global_context.py [ln:1-5,15-18] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="8-9"
    {!> docs_src/getting_started/context/redis/custom_global_context.py [ln:1-5,15-18] !}
    ```

Afterward, you can access your `secret` field in the usual way:

=== "AIOKafka"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/context/kafka/custom_global_context.py [ln:8-13.44] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/context/confluent/custom_global_context.py [ln:8-13.44] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/context/rabbit/custom_global_context.py [ln:8-13.44] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/context/nats/custom_global_context.py [ln:8-13.44] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/context/redis/custom_global_context.py [ln:8-13.44] !}
    ```

In this case, the field becomes a global context field: it does not depend on the current message handler (unlike `message`)

To remove a field from the context use the `reset_global` method:

```python
context.reset_global("my_key")
```

## Local

To set a local context (available only within the message processing scope), use the context manager `scope`

=== "AIOKafka"
    ```python linenums="1" hl_lines="15 19 21-22"
    {!> docs_src/getting_started/context/kafka/custom_local_context.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="15 19 21-22"
    {!> docs_src/getting_started/context/confluent/custom_local_context.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="15 19 21-22"
    {!> docs_src/getting_started/context/rabbit/custom_local_context.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="15 19 21-22"
    {!> docs_src/getting_started/context/nats/custom_local_context.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="15 19 21-22"
    {!> docs_src/getting_started/context/redis/custom_local_context.py !}
    ```

You can also set the context by yourself, and it will remain within the current call stack until you clear it.

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 14 25"
    {!> docs_src/getting_started/context/kafka/manual_local_context.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 14 25"
    {!> docs_src/getting_started/context/confluent/manual_local_context.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 14 25"
    {!> docs_src/getting_started/context/rabbit/manual_local_context.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 14 25"
    {!> docs_src/getting_started/context/nats/manual_local_context.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 14 25"
    {!> docs_src/getting_started/context/redis/manual_local_context.py !}
    ```
