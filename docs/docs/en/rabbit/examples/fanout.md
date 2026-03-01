---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Fanout Exchange

The **Fanout** Exchange is an even simpler, but slightly less popular way of routing in *RabbitMQ*. This type of `exchange` sends messages to all queues subscribed to it, ignoring any arguments of the message.

At the same time, if the queue listens to several consumers, messages will also be distributed among them (default [scaling mechanism](../direct#scaling){.internal-link}).

## Example

```python linenums="1"
{! docs_src/rabbit/subscription/fanout.py !}
```

### Consumer Announcement

To begin with, we announced our **Fanout** exchange and several queues that will listen to it:

```python linenums="7" hl_lines="1"
{! docs_src/rabbit/subscription/fanout.py [ln:7-10] !}
```

Then we signed up several consumers using the advertised queues to the `exchange` we created:

```python linenums="13" hl_lines="1 6 11"
{! docs_src/rabbit/subscription/fanout.py [ln:13-25] !}
```

!!! note
    `handler1` and `handler2` are subscribed to the same `exchange` using the same queue:
    within a single service, this does not make sense, since messages will come to these handlers in turn.
    Here we emulate the work of several consumers and load balancing between them.

### Message Distribution

Now the all messages will be send to all subscribers due they are binded to the same **FANOUT** exchange:

```python linenums="30"
{! docs_src/rabbit/subscription/fanout.py [ln:30.5,31.5,32.5,33.5] !}
```

---

!!! note
    When sending messages to **Fanout** exchange, it makes no sense to specify the arguments `queue` or `routing_key`, because they will be ignored.
