---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Application-level Filtering

**FastStream** also allows you to specify the message processing way using message headers, body type or something else. The `filter` feature enables you to consume various messages with different schemas within a single event stream.

!!! tip
    Message must be consumed at ONCE (crossing filters are not allowed)

As an example, let's create a subscriber for both `JSON` and non-`JSON` messages:

=== "AIOKafka"
    ```python linenums="1" hl_lines="7 9-11 16"
    {!> docs_src/getting_started/subscription/kafka/filter.py [ln:1-18] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="7 9-11 16"
    {!> docs_src/getting_started/subscription/confluent/filter.py [ln:1-18] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="7 9-11 16"
    {!> docs_src/getting_started/subscription/rabbit/filter.py [ln:1-18] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="7 9-11 16"
    {!> docs_src/getting_started/subscription/nats/filter.py [ln:1-18] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="7 9-11 16"
    {!> docs_src/getting_started/subscription/redis/filter.py [ln:1-18] !}
    ```

!!! note
    A subscriber without a filter is a default subscriber. It consumes messages that have not been consumed yet.

For now, the following message will be delivered to the `handle` function

=== "AIOKafka"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/kafka/filter.py [ln:24.5,25.5,26.5,27.5] !}
    ```

=== "Confluent"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/confluent/filter.py [ln:24.5,25.5,26.5,27.5] !}
    ```

=== "RabbitMQ"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/rabbit/filter.py [ln:24.5,25.5,26.5,27.5] !}
    ```

=== "NATS"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/nats/filter.py [ln:24.5,25.5,26.5,27.5] !}
    ```

=== "Redis"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/redis/filter.py [ln:24.5,25.5,26.5,27.5] !}
    ```

And this one will be delivered to the `default_handler`

=== "AIOKafka"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/kafka/filter.py [ln:29.5,30.5,31.5,32.5] !}
    ```

=== "Confluent"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/confluent/filter.py [ln:29.5,30.5,31.5,32.5] !}
    ```

=== "RabbitMQ"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/rabbit/filter.py [ln:29.5,30.5,31.5,32.5] !}
    ```

=== "NATS"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/nats/filter.py [ln:29.5,30.5,31.5,32.5] !}
    ```

=== "Redis"
    ```python hl_lines="2"
    {!> docs_src/getting_started/subscription/redis/filter.py [ln:29.5,30.5,31.5,32.5] !}
    ```

---

## Technical Information

Let's break down how message filtering works in a subscription mechanism.

### Core Filtering Logic

Consider a simple example of a filter implementation:

```python
for handler in subscriber.handlers:
    if await handler.filter(msg):
        return await handler.process(msg)

raise HandlerNotFoundError
```

This code selects the first suitable handler to process the message. This means the **default handler should be placed last** in the list. If no logical handlers match, the message must still be processed. For this, we need a special trash handler that defines the system's default behavior for such cases.

### Implementing the Default Handler

The default handler should be declared as follows:

```python
subscriber = broker.subscriber()

@subscriber(filter=...)
async def handler(): ...

@subscriber()
async def default_handler(): ...
```

Here, `@subscriber()` is equivalent to `@subscriber(filter=lambda _: True)`, meaning it **accepts all** messages. This ensures that no message goes unprocessed, even if no specific handler is found.

### Summary

- Handlers are checked in order, and the first matching one processes the message.
- The default handler must be **placed last** to ensure all messages are handled.
- `@subscriber()` without parameters acts as a universal handler, accepting everything.
- A trash handler must properly finalize the subscription and inform the broker about unnecessary data.

Properly managing subscribers allows for precise message processing control and prevents data loss.
