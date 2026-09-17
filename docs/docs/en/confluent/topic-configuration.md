---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Topic Configuration

By default, **FastStream** creates every topic your subscribers consume from with a single partition and a replication factor of `1`. To configure a topic, pass a `Topic` object instead of a plain topic name:

```python linenums="1" hl_lines="2 9"
{! docs_src/confluent/topic_configuration/app.py !}
```

`Topic` and plain strings can be mixed freely — a string is just a shortcut for `#!python Topic(name)`, so `#!python "audit"` above is created with the defaults.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `num_partitions` | `1` | Number of partitions to create the topic with. |
| `replication_factor` | `1` | Replication factor to create the topic with. |
| `declare` | `True` | Whether **FastStream** creates the topic for you. |

Settings apply at creation time only: **Kafka** ignores them for a topic that already exists, so changing `num_partitions` will not repartition a live topic.

## Opting a Topic out of Creation

Set `#!python declare=False` for topics that somebody else provisions — another service, or your infrastructure-as-code:

```python linenums="1" hl_lines="10"
{! docs_src/confluent/topic_configuration/app.py !}
```

**FastStream** then simply skips the creation request for that topic. It does not check whether the topic exists and does not fail if it is missing, so your consumer starts either way.

!!! note
    `#!python declare=False` narrows creation down for a single topic. To turn topic creation off for the whole broker, use `#!python KafkaBroker(allow_auto_create_topics=False)` — that flag always wins, and no topic is created regardless of its `declare` value.

## Publishers

`#!python @broker.publisher(...)` accepts a `Topic` too, for symmetry with subscribers. **FastStream** never creates publisher topics, though, so only the topic name is used and the creation settings are ignored.

!!! warning
    Topic configuration is a `faststream.confluent` feature. `faststream.kafka` never creates topics — with **aiokafka**, topic creation is entirely up to the **Kafka** server's `auto.create.topics.enable` setting.
