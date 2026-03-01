---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Header Exchange

The **Header** Exchange is the most complex and flexible way to route messages in *RabbitMQ*. This `exchange` type sends messages to queues according by matching the queue binding arguments with message headers.

At the same time, if the queue listens to several consumers, messages will also be distributed among them (default [scaling mechanism](../direct#scaling){.internal-link}).

## Example

```python linenums="1"
{! docs_src/rabbit/subscription/header.py !}
```

### Consumer Announcement

First, we announce our **Header** exchange and several queues that will listen to it:

```python linenums="7" hl_lines="1 6 11 16"
{! docs_src/rabbit/subscription/header.py [ln:7-23] !}
```

The `x-match` argument indicates whether the arguments should match the message headers in whole or in part.

Then we signed up several consumers using the advertised queues to the `exchange` we created:

```python linenums="26" hl_lines="1 6 11 16"
{! docs_src/rabbit/subscription/header.py [ln:26-43] !}
```

!!! note
    `handler1` and `handler2` are subscribed to the same `exchange` using the same queue:
    within a single service, this does not make sense, since messages will come to these handlers in turn.
    Here we emulate the work of several consumers and load balancing between them.

### Message Distribution

Now the distribution of messages between these consumers will look like this:

```python linenums="48"
{! docs_src/rabbit/subscription/header.py [ln:48.5] !}
```

Message `1` will be sent to `handler1` because it listens to a queue whose `#!python "key"` header matches the `#!python "key"` header of the message.

---

```python linenums="49"
{! docs_src/rabbit/subscription/header.py [ln:49.5]!}
```

Message `2` will be sent to `handler2` because it listens to `exchange` using the same queue, but `handler1` is busy.

---

```python linenums="50"
{! docs_src/rabbit/subscription/header.py [ln:50.5]!}
```

Message `3` will be sent to `handler1` again because it is currently free.

---

```python linenums="51"
{! docs_src/rabbit/subscription/header.py [ln:51.5]!}
```

Message `4` will be sent to `handler3` because it listens to a queue whose `#!python "key"` header coincided with the `#!python "key"` header of the message.

---

```python linenums="52"
{! docs_src/rabbit/subscription/header.py [ln:52.5]!}
```

Message `5` will be sent to `handler3` because it listens to a queue whose header `#!python "key2"` coincided with the header `#!python "key2"` of the message.

---

```python linenums="53"
{! docs_src/rabbit/subscription/header.py [ln:53.5,54.5,55.5]!}
```

Message `6` will be sent to `handler3` and `handler4` because the message headers completely match the queue keys.

---

!!! note
    When sending messages to **Header** exchange, it makes no sense to specify the arguments `queue` or `routing_key`, because they will be ignored

!!! warning
    For incredibly complex routes, you can use the option to bind an `exchange` to another `exchange`. In this case, all the same rules apply as for queues subscribed to `exchange`. The only difference is that the signed `exchange` can further distribute messages according to its own rules.

    So, for example, you can combine Topic and Header exchange types.
