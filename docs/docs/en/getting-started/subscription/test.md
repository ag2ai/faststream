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

=== "MQTT"
    ```python linenums="1" title="annotation_redis.py"
    {!> docs_src/getting_started/subscription/mqtt/annotation.py !}
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
    {!> docs_src/getting_started/subscription/kafka/testing.py [ln:1-3,5,9-13] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/confluent/testing.py [ln:1-3,5,9-13] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/rabbit/testing.py [ln:1-3,5,9-13] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/nats/testing.py [ln:1-3,5,9-13] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/redis/testing.py [ln:1-3,5,9-13] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="4 8-9"
    {!> docs_src/getting_started/subscription/mqtt/testing.py [ln:1-3,5,9-13] !}
    ```

### Catching Exceptions

This way you can catch any exceptions that occur inside your handler:

=== "AIOKafka"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/kafka/testing.py [ln:20-26] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/confluent/testing.py [ln:20-26] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/rabbit/testing.py [ln:20-26] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/nats/testing.py [ln:20-26] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/redis/testing.py [ln:20-26] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="4"
    {!> docs_src/getting_started/subscription/mqtt/testing.py [ln:20-26] !}
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

=== "MQTT"
    ```python linenums="1" hl_lines="4 8-9 14"
    {!> docs_src/getting_started/subscription/mqtt/full_testing.py !}
    ```

### Validates Input

Also, all handlers in test mode have an extra [`MagicMock`](https://docs.python.org/3/library/unittest.mock.html#unittest.mock.MagicMock){.external-link target="_blank"} object to validate passed arguments and call counts.

=== "AIOKafka"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/kafka/testing.py [ln:10-15] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/confluent/testing.py [ln:10-15] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/rabbit/testing.py [ln:10-15] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/nats/testing.py [ln:10-15] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/redis/testing.py [ln:10-15] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="6"
    {!> docs_src/getting_started/subscription/mqtt/testing.py [ln:10-15] !}
    ```

!!! note
    The *handle* mock has a raw **JSON** message body. This way you can validate the incoming message itself and not a parsed python arguments.

    Thus our example checks not `#!python mock.assert_called_with(name="John", user_id=1)`, but `#!python mock.assert_called_with({ "name": "John", "user_id": 1 })`.

Scoping rule: the mock exists only inside the context manager. Once it exits, `handle.mock` raises a `SetupError` instead of answering for calls nobody made.

=== "AIOKafka"
    ```python linenums="1" hl_lines="6 8-9"
    {!> docs_src/getting_started/subscription/kafka/testing.py [ln:10-18] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="6 8-9"
    {!> docs_src/getting_started/subscription/confluent/testing.py [ln:10-18] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="6 8-9"
    {!> docs_src/getting_started/subscription/rabbit/testing.py [ln:10-18] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="6 8-9"
    {!> docs_src/getting_started/subscription/nats/testing.py [ln:10-18] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="6 8-9"
    {!> docs_src/getting_started/subscription/redis/testing.py [ln:10-18] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="6 8-9"
    {!> docs_src/getting_started/subscription/mqtt/testing.py [ln:10-18] !}
    ```

### Validates Message Fields

Every handler also has an `assert_called_once_with` method. It checks the message body the same way `mock.assert_called_once_with` does, and beside it the message fields the handler saw: `headers`, `correlation_id`, `reply_to`, `content_type` and `path`.

Let's take an example of such an application:

=== "AIOKafka"
    ```python linenums="1"
    {!> docs_src/getting_started/subscription/kafka/advanced_testing.py [ln:1-2,5-26] !}
    ```

=== "Confluent"
    ```python linenums="1"
    {!> docs_src/getting_started/subscription/confluent/advanced_testing.py [ln:1-2,5-26] !}
    ```

=== "RabbitMQ"
    ```python linenums="1"
    {!> docs_src/getting_started/subscription/rabbit/advanced_testing.py [ln:1-2,5-26] !}
    ```

=== "NATS"
    ```python linenums="1"
    {!> docs_src/getting_started/subscription/nats/advanced_testing.py [ln:1-2,5-26] !}
    ```

=== "Redis"
    ```python linenums="1"
    {!> docs_src/getting_started/subscription/redis/advanced_testing.py [ln:1-2,5-26] !}
    ```

=== "MQTT"
    ```python linenums="1"
    {!> docs_src/getting_started/subscription/mqtt/advanced_testing.py [ln:1-2,5-26] !}
    ```

Using `assert_called_once_with`, you can check the body and the headers in one statement. The body may be a plain `dict` or your model: it goes through the broker codec before the comparison, so both spellings mean the same message.

=== "AIOKafka"
    ```python linenums="1" hl_lines="16-19 21-24"
    {!> docs_src/getting_started/subscription/kafka/advanced_testing.py [ln:3,5-8,28-46] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="16-19 21-24"
    {!> docs_src/getting_started/subscription/confluent/advanced_testing.py [ln:3,5-8,28-46] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="16-19 21-24"
    {!> docs_src/getting_started/subscription/rabbit/advanced_testing.py [ln:3,5-8,28-46] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="16-19 21-24"
    {!> docs_src/getting_started/subscription/nats/advanced_testing.py [ln:3,5-8,28-46] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="16-19 21-24"
    {!> docs_src/getting_started/subscription/redis/advanced_testing.py [ln:3,5-8,28-46] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="16-19 21-24"
    {!> docs_src/getting_started/subscription/mqtt/advanced_testing.py [ln:3,5-8,28-46] !}
    ```

Headers match as a subset: FastStream adds its own headers (`content-type`, `correlation_id`) beside yours, and they never get in the way. Every other field matches exactly. When several fields differ, the `AssertionError` lists all of them at once.

A handler that saw several messages answers with two more methods, named as in `unittest.mock` and taking the same arguments: `assert_called_with` checks the last message, as the mock's does, and `assert_any_call` passes when any of the messages matches. When none does, the `AssertionError` lists every message the handler saw with its own mismatches.

=== "AIOKafka"
    ```python linenums="1" hl_lines="24-27 29-32"
    {!> docs_src/getting_started/subscription/kafka/advanced_testing.py [ln:3,5-8,64-90] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="24-27 29-32"
    {!> docs_src/getting_started/subscription/confluent/advanced_testing.py [ln:3,5-8,63-89] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="24-27 29-32"
    {!> docs_src/getting_started/subscription/rabbit/advanced_testing.py [ln:3,5-8,63-89] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="24-27 29-32"
    {!> docs_src/getting_started/subscription/nats/advanced_testing.py [ln:3,5-8,63-89] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="24-27 29-32"
    {!> docs_src/getting_started/subscription/redis/advanced_testing.py [ln:3,5-8,63-89] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="24-27 29-32"
    {!> docs_src/getting_started/subscription/mqtt/advanced_testing.py [ln:3,5-8,63-89] !}
    ```

To check only a part of the body, put a [dirty-equals](https://dirty-equals.helpmanual.io/){.external-link target="_blank"} matcher in its place. Anything the fields above do not cover, such as the Kafka message key, lives in the context: check it through `context` by the same path you would give to `Context()`, walking attributes and dict keys from a context name.

=== "AIOKafka"
    ```python linenums="1" hl_lines="2 15 19-20"
    {!> docs_src/getting_started/subscription/kafka/advanced_testing.py [ln:3-8,48-62] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="2 18-19"
    {!> docs_src/getting_started/subscription/confluent/advanced_testing.py [ln:3-8,48-61] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="2 18-19"
    {!> docs_src/getting_started/subscription/rabbit/advanced_testing.py [ln:3-8,48-61] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="2 18-19"
    {!> docs_src/getting_started/subscription/nats/advanced_testing.py [ln:3-8,48-61] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="2 18-19"
    {!> docs_src/getting_started/subscription/redis/advanced_testing.py [ln:3-8,48-61] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="2 18-19"
    {!> docs_src/getting_started/subscription/mqtt/advanced_testing.py [ln:3-8,48-61] !}
    ```

!!! note
    A context path reads attributes and keys, it never calls. Where a raw message answers with methods, as the Confluent one does, reach for what **FastStream** put in the context beside it, such as `log_context`.

!!! note
    Both `handle.mock` and the three assertion methods exist only inside the test broker. Outside of it they raise a `SetupError` instead of answering for a handler nobody has called.


## Real Broker Testing

If you want to test your application in a real environment, you shouldn't have to rewrite all your tests: just pass `with_real` optional parameter to your `TestClient` context manager. This way, `TestClient` supports all the testing features but uses an unpatched broker to send and consume messages.

=== "AIOKafka"
    ```python linenums="1" hl_lines="5 9 11 19 22"
    {!> docs_src/getting_started/subscription/kafka/real_testing.py [ln:1-6,10-27] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="5 9 11 19 22"
    {!> docs_src/getting_started/subscription/confluent/real_testing.py [ln:1-6,10-27] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="5 9 11 19 22"
    {!> docs_src/getting_started/subscription/rabbit/real_testing.py [ln:1-6,10-27] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="5 9 11 19 22"
    {!> docs_src/getting_started/subscription/nats/real_testing.py [ln:1-6,10-27] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="5 9 11 19 22"
    {!> docs_src/getting_started/subscription/redis/real_testing.py [ln:1-6,10-27] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="5 9 11 19 22"
    {!> docs_src/getting_started/subscription/mqtt/real_testing.py [ln:1-6,10-27] !}
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
