---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Pydantic Serialization

## pydantic.Field

Besides, **FastStream** uses your handlers' annotations to collect information about the application schema and generate [**AsyncAPI**](https://www.asyncapi.com){.external-link target="_blank"} schema.

You can access this information with extra details using `pydantic.Field` (such as title, description and examples). Additionally, [**Fields**](https://docs.pydantic.dev/latest/usage/fields/){.external-link target="_blank"} usage allows you to add extra validations to your message schema.

Just use `pydantic.Field` as a function default argument:

=== "AIOKafka"
    ```python linenums="1" hl_lines="12-17"
    {!> docs_src/getting_started/subscription/kafka/pydantic_fields.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="12-17"
    {!> docs_src/getting_started/subscription/confluent/pydantic_fields.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="12-17"
    {!> docs_src/getting_started/subscription/rabbit/pydantic_fields.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="12-17"
    {!> docs_src/getting_started/subscription/nats/pydantic_fields.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="12-17"
    {!> docs_src/getting_started/subscription/redis/pydantic_fields.py !}
    ```


!!! tip
    Also you can use `typing.Annotated` (python 3.9+) or `typing_extensions.Annotated` to declare your handler fields

    ```python
    {!> docs_src/getting_started/subscription/kafka/pydantic_annotated_fields.py [ln:14.5,15.5,16.5,17.5,18.5,19.5,20.5,21.5] !}
    ```

## pydantic.BaseModel

To make your message schema reusable between different subscribers and publishers, you can declare it as a `pydantic.BaseModel` and use it as a single message annotation:

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 10 21"
    {!> docs_src/getting_started/subscription/kafka/pydantic_model.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 10 21"
    {!> docs_src/getting_started/subscription/confluent/pydantic_model.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 10 21"
    {!> docs_src/getting_started/subscription/rabbit/pydantic_model.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 10 21"
    {!> docs_src/getting_started/subscription/nats/pydantic_model.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 10 21"
    {!> docs_src/getting_started/subscription/redis/pydantic_model.py !}
    ```
