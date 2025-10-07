---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publisher Testing

If you are working with a Publisher object (either as a decorator or directly), you have several testing features available:

* In-memory TestClient
* Publishing locally with error propagation
* Checking the incoming message body

## Base Application

Let's take a look at a simple application example with a publisher as a decorator or as a direct call:

=== "Decorator"

    === "AIOKafka"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/kafka/object.py [ln:7-12] !}
        ```

    === "Confluent"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/confluent/object.py [ln:7-12] !}
        ```

    === "RabbitMQ"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/rabbit/object.py [ln:7-12] !}
        ```

    === "NATS"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/nats/object.py [ln:7-12] !}
        ```

    === "Redis"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/redis/object.py [ln:7-12] !}
        ```


=== "Direct"
    === "AIOKafka"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/kafka/direct.py [ln:7-11] !}
        ```

    === "Confluent"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/confluent/direct.py [ln:7-11] !}
        ```

    === "RabbitMQ"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/rabbit/direct.py [ln:7-11] !}
        ```

    === "NATS"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/nats/direct.py [ln:7-11] !}
        ```

    === "Redis"
        ```python linenums="1"
        {!> docs_src/getting_started/publishing/redis/direct.py [ln:7-11] !}
        ```


## Testing

To test it, you just need to patch your broker with a special *TestBroker*.

=== "AIOKafka"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/publishing/kafka/object_testing.py [ln:1-4,8-12] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/publishing/confluent/object_testing.py [ln:1-4,8-12] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/publishing/rabbit/object_testing.py [ln:1-4,8-12] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/publishing/nats/object_testing.py [ln:1-4,8-12] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/publishing/redis/object_testing.py [ln:1-4,8-12] !}
    ```

By default, it patches your broker to run **In-Memory**, so you can use it without any external broker. It should be extremely useful in your CI or local development environment.

Also, it allows you to check the outgoing message body in the same way as with a [subscriber](../subscription/test.md#validates-input){.internal-link}.

```python
publisher.mock.assert_called_once_with("Hi!")
```

!!! note
    The Publisher mock contains not just a `publish` method input value. It sets up a virtual consumer for an outgoing topic, consumes a message, and stores this consumed one.

!!! note
    In order for publishers to be properly patched by the test broker, you need to create them before running the test broker

Additionally, *TestBroker* can be used with a real external broker to make your tests end-to-end suitable. For more information, please visit the [subscriber testing page](../subscription/test.md#real-broker-testing){.internal-link}.
