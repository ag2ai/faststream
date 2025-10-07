---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Subscriber Testing

Testability is a crucial part of any application, and **FastStream** provides you with the tools to test your code easily.

## Original Application

Let's take a look at the original application to test

=== "AIOKafka"
    ```python linenums="1" title="annotation_kafka.py"
    {!> docs_src/getting_started/subscription/kafka/annotation.py !}
    ```

=== "Confluent"
    ```python linenums="1" title="annotation_confluent.py"
    {!> docs_src/getting_started/subscription/confluent/annotation.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" title="annotation_rabbit.py"
    {!> docs_src/getting_started/subscription/rabbit/annotation.py !}
    ```

=== "NATS"
    ```python linenums="1" title="annotation_nats.py"
    {!> docs_src/getting_started/subscription/nats/annotation.py !}
    ```

=== "Redis"
    ```python linenums="1" title="annotation_redis.py"
    {!> docs_src/getting_started/subscription/redis/annotation.py !}
    ```

It consumes **JSON** messages like `#!json { "name": "username", "user_id": 1 }`

You can test your consume function like a regular one, for sure:

```python
@pytest.mark.asyncio
async def test_handler():
    await handle("John", 1)
```

But if you want to test your function closer to your real runtime, you should use the special **FastStream** test client.

## In-Memory Testing

Deploying a whole service with a Message Broker is a bit too much just for testing purposes, especially in your CI environment. Not to mention the possible loss of messages due to network failures when working with real brokers.

For this reason, **FastStream** has a special `TestClient` to make your broker work in `InMemory` mode.

Just use it like a regular async context manager - all published messages will be routed in-memory (without any external dependencies) and consumed by the correct handler.

=== "AIOKafka"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/kafka/testing.py [ln:1-4,8-12] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/confluent/testing.py [ln:1-4,8-12] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/rabbit/testing.py [ln:1-4,8-12] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/nats/testing.py [ln:1-4,8-12] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/redis/testing.py [ln:1-4,8-12] !}
    ```

### Catching Exceptions

This way you can catch any exceptions that occur inside your handler:

=== "AIOKafka"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/kafka/testing.py [ln:18-23] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/confluent/testing.py [ln:18-23] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/rabbit/testing.py [ln:18-23] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/nats/testing.py [ln:18-23] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/redis/testing.py [ln:18-23] !}
    ```

## Full Example
Let's look at a complete example of creating an app and testing it

=== "AIOKafka"
    ```python linenums="1" hl_lines="4 8-9 14"
    {!> docs_src/getting_started/subscription/kafka/full_testing.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4 8-9 14"
    {!> docs_src/getting_started/subscription/confluent/full_testing.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4 8-9 14"
    {!> docs_src/getting_started/subscription/rabbit/full_testing.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4 8-9 14"
    {!> docs_src/getting_started/subscription/nats/full_testing.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4 8-9 14"
    {!> docs_src/getting_started/subscription/redis/full_testing.py !}
    ```

### Validates Input

Also, all handlers in test mode have an extra [`MagicMock`](https://docs.python.org/3/library/unittest.mock.html#unittest.mock.MagicMock){.external-link target="_blank"} object to validate passed arguments and call counts.

=== "AIOKafka"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/kafka/testing.py [ln:9-14] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/confluent/testing.py [ln:9-14] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/rabbit/testing.py [ln:9-14] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/nats/testing.py [ln:9-14] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/redis/testing.py [ln:9-14] !}
    ```

!!! note
    The *handle* mock has a raw **JSON** message body. This way you can validate the incoming message itself and not a parsed python arguments.

    Thus our example checks not `#!python mock.assert_called_with(name="John", user_id=1)`, but `#!python mock.assert_called_with({ "name": "John", "user_id": 1 })`.

Scoping rule: all handles' mock objects will be cleared when the context manager exits.

=== "AIOKafka"
    ```python linenums="1" hl_lines="6 8"
    {!> docs_src/getting_started/subscription/kafka/testing.py [ln:9-16] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="6 8"
    {!> docs_src/getting_started/subscription/confluent/testing.py [ln:9-16] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="6 8"
    {!> docs_src/getting_started/subscription/rabbit/testing.py [ln:9-16] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="6 8"
    {!> docs_src/getting_started/subscription/nats/testing.py [ln:9-16] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="6 8"
    {!> docs_src/getting_started/subscription/redis/testing.py [ln:9-16] !}
    ```

## Real Broker Testing

If you want to test your application in a real environment, you shouldn't have to rewrite all your tests: just pass `with_real` optional parameter to your `TestClient` context manager. This way, `TestClient` supports all the testing features but uses an unpatched broker to send and consume messages.

=== "AIOKafka"
    ```python linenums="1" hl_lines="4 8 10 17 20"
    {!> docs_src/getting_started/subscription/kafka/real_testing.py [ln:1-5,9-25] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4 8 10 17 20"
    {!> docs_src/getting_started/subscription/confluent/real_testing.py [ln:1-5,9-25] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4 8 10 17 20"
    {!> docs_src/getting_started/subscription/rabbit/real_testing.py [ln:1-5,9-25] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4 8 10 17 20"
    {!> docs_src/getting_started/subscription/nats/real_testing.py [ln:1-5,9-25] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4 8 10 17 20"
    {!> docs_src/getting_started/subscription/redis/real_testing.py [ln:1-5,9-25] !}
    ```

!!! tip
    When you're using a patched broker to test your consumers, the publish method is called synchronously with a consumer one, so you need not wait until your message is consumed. But in the real broker's case, it doesn't.

    For this reason, you have to wait for message consumption manually with the special `#!python handler.wait_call(timeout)` method.
    Also, inner handler exceptions will be raised in this function, not `#!python broker.publish(...)`.

### A Little Tip

It can be very useful to set the `with_real` flag using an environment variable. This way, you will be able to choose the testing mode right from the command line:

```bash
WITH_REAL=True/False pytest ...
```

To learn more about managing your application configuration visit [this page](../config/index.md){.internal-link}.
