---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Direct

The **Direct** Subject is the basic way to route messages in *NATS*. Its essence is very simple:
a `subject` sends messages to all consumers subscribed to it.

## Scaling

{! includes/en/nats/scaling.md !}

## Example

The **Direct** Subject is the type used in **FastStream** by default: you can simply declare it as follows

```python
@broker.handler("test_subject")
async def handler():
...
```

Full example:

```python linenums="1"
{! docs_src/nats/direct.py [ln:1-12.42,13-] !}
```

### Consumer Announcement

To begin with, we have declared several consumers for two `subjects`: `#!python "test-subj-1"` and `#!python "test-subj-2"`:

```python linenums="7" hl_lines="1 5 9"
{! docs_src/nats/direct.py [ln:7-12.42,13-17] !}
```

!!! note
    Note that all consumers are subscribed using the same `queue_group`. Within the same service, this does not make sense, since messages will come to these handlers in turn.
    Here, we emulate the work of several consumers and load balancing between them.

### Message Distribution

Now the distribution of messages between these consumers will look like this:

```python
{! docs_src/nats/direct.py [ln:21.5] !}
```

The message `1` will be sent to `handler1` or `handler2` because they are listening to one `#!python "test-subj-1"` `subject` within one `queue group`.

---

```python
{! docs_src/nats/direct.py [ln:22.5] !}
```

Message `2` will be sent similarly to message `1`.

---

```python
{! docs_src/nats/direct.py [ln:23.5] !}
```

The message `3` will be sent to `handler3` because it is the only one listening to `#!python "test-subj-2"`.
