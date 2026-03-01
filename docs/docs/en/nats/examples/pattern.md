---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Pattern

[**Pattern**](https://docs.nats.io/nats-concepts/subjects#wildcards){.external-link target="_blank"} Subject is a powerful *NATS* routing engine. This type of `subject` routes messages to consumers based on the *pattern* specified when they connect to the `subject` and a message key.

## Scaling

{! includes/en/nats/scaling.md !}

## Example

```python linenums="1"
{! docs_src/nats/pattern.py [ln:1-12.42,13-] !}
```

### Consumer Announcement

To begin with, we have announced several consumers for two `subjects`: `#!python "*.info"` and `#!python "*.error"`:

```python linenums="7" hl_lines="1 5 9"
{! docs_src/nats/pattern.py [ln:7-12.42,13-17] !}
```

At the same time, in the `subject` of our consumers, we specify the *pattern* that will be processed by these consumers.

!!! note
    Note that all consumers are subscribed using the same `queue_group`. Within the same service, this does not make sense, since messages will come to these handlers in turn.
    Here, we emulate the work of several consumers and load balancing between them.

### Message Distribution

Now the distribution of messages between these consumers will look like this:

```python
{! docs_src/nats/pattern.py [ln:21.5] !}
```

The message `1` will be sent to `handler1` or `handler2` because they listen to the same `subject` template within the same `queue group`.

---

```python
{! docs_src/nats/pattern.py [ln:22.5] !}
```

Message `2` will be sent similarly to message `1`.

---

```python
{! docs_src/nats/pattern.py [ln:23.5] !}
```

The message `3` will be sent to `handler3` because it is the only one listening to the pattern `#!python "*.error"`.
