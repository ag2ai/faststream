---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Access by Name

Sometimes, you may need to use a different name for the argument (not the one under which it is stored in the context) or get access to specific parts of the object. To do this, simply specify the name of what you want to access, and the context will provide you with the object.

=== "AIOKafka"
    ```python linenums="1" hl_lines="11-13"
    {!> docs_src/getting_started/context/kafka/fields_access.py !}
    ```

    This way you can get access to context object by its name

    ```python
    {!> docs_src/getting_started/context/kafka/fields_access.py [ln:11.5] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="11-13"
    {!> docs_src/getting_started/context/confluent/fields_access.py !}
    ```

    This way you can get access to context object by its name

    ```python
    {!> docs_src/getting_started/context/confluent/fields_access.py [ln:11.5] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="11-13"
    {!> docs_src/getting_started/context/rabbit/fields_access.py !}
    ```

    This way you can get access to context object by its name

    ```python
    {!> docs_src/getting_started/context/rabbit/fields_access.py [ln:11.5] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="11-13"
    {!> docs_src/getting_started/context/nats/fields_access.py !}
    ```

    This way you can get access to context object by its name

    ```python
    {!> docs_src/getting_started/context/nats/fields_access.py [ln:11.5] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="11-13"
    {!> docs_src/getting_started/context/redis/fields_access.py !}
    ```

    This way you can get access to context object by its name

    ```python
    {!> docs_src/getting_started/context/redis/fields_access.py [ln:11.5] !}
    ```

This way you can get access to context object specific field


```python
{! docs_src/getting_started/context/kafka/fields_access.py [ln:12.5] !}
```

Or even to a dict key


```python
{! docs_src/getting_started/context/kafka/fields_access.py [ln:13.5] !}
```
