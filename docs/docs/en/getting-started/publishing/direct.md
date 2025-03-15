---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publisher Direct Usage

The Publisher Object provides a full-featured way to publish messages. It has an [**AsyncAPI**](../asyncapi/custom.md){.internal-link} representation and includes [testability](./test.md){.internal-link} features.

This method creates a reusable Publisher object that can be used directly to publish a message:

=== "AIOKafka"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/kafka/direct.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/confluent/direct.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/rabbit/direct.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/nats/direct.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="7 11"
    {!> docs_src/getting_started/publishing/redis/direct.py !}
    ```

It is something in the middle between [broker publish](./broker.md){.internal-link} and [object decorator](./object.md){.internal-link}. It has an **AsyncAPI** representation and *testability* features (like the **object decorator**), but allows you to send different messages to different outputs (like the **broker publish**).

```python hl_lines="3-4"
@broker.subscriber("in")
async def handle(msg) -> str:
    await publisher1.publish("Response-1")
    await publisher2.publish("Response-2")
```

!!! note
    When using this method, **FastStream** doesn't reuse the incoming `correlation_id` to mark outgoing messages with it. You should set it manually if it is required.
