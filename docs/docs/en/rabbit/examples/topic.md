---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Topic Exchange

The **Topic** Exchange is a powerful *RabbitMQ* routing tool. This type of `exchange` sends messages to the queue in accordance with the *pattern* specified when they are connected to `exchange` and the `routing_key` of the message itself.

At the same time, if the queue listens to several consumers, messages will also be distributed among them (default [scaling mechanism](../direct#scaling){.internal-link}).

## Example

```python linenums="1"
{! docs_src/rabbit/subscription/topic.py !}
```

### Consumer Announcement

First, we announce our **Topic** exchange and several queues that will listen to it:

```python linenums="7" hl_lines="1 3-4"
{! docs_src/rabbit/subscription/topic.py [ln:7-10]!}
```

At the same time, in the `routing_key` of our queues, we specify the *pattern* of routing keys that will be processed by this queue.

Then we sign up several consumers using the advertised queues to the `exchange` we created:

```python linenums="13" hl_lines="1 6 11"
{! docs_src/rabbit/subscription/topic.py [ln:13-25]!}
```

!!! note
    `handler1` and `handler2` are subscribed to the same `exchange` using the same queue:
    within a single service, this does not make sense, since messages will come to these handlers in turn.
    Here we emulate the work of several consumers and load balancing between them.

### Message Distribution

Now the distribution of messages between these consumers will look like this:

```python linenums="30"
{! docs_src/rabbit/subscription/topic.py [ln:30.5]!}
```

Message `1` will be sent to `handler1` because it listens to `#!python "exchange"` using a queue with the routing key `#!python "*.info"`.

---

```python linenums="31"
{! docs_src/rabbit/subscription/topic.py [ln:31.5]!}
```

Message `2` will be sent to `handler2` because it listens to `#!python "exchange"` using the same queue, but `handler1` is busy.

---

```python linenums="32"
{! docs_src/rabbit/subscription/topic.py [ln:32.5]!}
```

Message `3` will be sent to `handler1` again because it is currently free.

---

```python linenums="33"
{! docs_src/rabbit/subscription/topic.py [ln:33.5]!}
```

Message `4` will be sent to `handler3` because it is the only one listening to `#!python "exchange"` using a queue with the routing key `#!python "*.debug"`.
