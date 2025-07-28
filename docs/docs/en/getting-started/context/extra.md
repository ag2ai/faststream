---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Context Extra Options

Additionally, `Context` provides you with some extra capabilities for working with containing objects.

## Default Values

For instance, if you attempt to access a field that doesn't exist in the global context, you will receive a `pydantic.ValidationError` exception.

However, you can set default values if needed.

=== "AIOKafka"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/kafka/default_arguments.py [ln:7-11] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/confluent/default_arguments.py [ln:7-11] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/rabbit/default_arguments.py [ln:7-11] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/nats/default_arguments.py [ln:7-11] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/redis/default_arguments.py [ln:7-11] !}
    ```

## Cast Context Types

By default, context fields are **NOT CAST** to the type specified in their annotation.

=== "AIOKafka"
    ```python linenums="1" hl_lines="7 12 14"
    {!> docs_src/getting_started/context/kafka/cast.py [ln:1-14] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="7 12 14"
    {!> docs_src/getting_started/context/confluent/cast.py [ln:1-14] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="7 12 14"
    {!> docs_src/getting_started/context/rabbit/cast.py [ln:1-14] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="7 12 14"
    {!> docs_src/getting_started/context/nats/cast.py [ln:1-14] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="7 12 14"
    {!> docs_src/getting_started/context/redis/cast.py [ln:1-14] !}
    ```

If you require this functionality, you can enable the appropriate flag.

=== "AIOKafka"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/kafka/cast.py [ln:15-19] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/confluent/cast.py [ln:15-19] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/rabbit/cast.py [ln:15-19] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/nats/cast.py [ln:15-19] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3 5"
    {!> docs_src/getting_started/context/redis/cast.py [ln:15-19] !}
    ```

## Initial Value

Also, `Context` provides you with a `initial` option to setup base context value without previous `set_global` call.

=== "AIOKafka"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/kafka/initial.py [ln:7-12] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/confluent/initial.py [ln:7-12] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/rabbit/initial.py [ln:7-12] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/nats/initial.py [ln:7-12] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4 6"
    {!> docs_src/getting_started/context/redis/initial.py [ln:7-12] !}
    ```
