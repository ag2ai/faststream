---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Multiple Brokers

Usually a **FastStream** application is built around a single broker, but sometimes one process needs to talk to more than one messaging system at the same time. A common case is **bridging** two systems: consume from one broker and re-publish to another. Another is a gradual **migration** from one broker to another, where both must run side by side for a while.

To support these scenarios, the `#!python FastStream` application accepts **multiple brokers** at once.

## Passing Brokers to the Application

Just pass all the brokers you want to run to the `#!python FastStream` constructor. Each broker keeps its own subscribers and publishers, and the application starts and stops all of them together.

In the example below we consume messages from **Kafka** and bridge them into **NATS**:

```python linenums="1" hl_lines="5-6 8 11-12 18"
{!> docs_src/getting_started/multiple_brokers/app.py !}
```

When the application runs, both brokers connect. The `#!python from_kafka` handler is registered on the `kafka_broker`, while the [`#!python @nats_broker.publisher`](./publishing/decorator.md){.internal-link} decorator routes its return value to the `nats_broker` — so a single message coming from **Kafka** ends up being delivered to a **NATS** subscriber.

Each broker is an independent object: you attach subscribers and publishers to the exact broker you mean, which makes routing between systems explicit.

## Adding Brokers Dynamically

If you don't have all brokers available at construction time, you can register them later with the `#!python add_broker` method. It is equivalent to passing the broker to the constructor:

```python linenums="1" hl_lines="9"
{!> docs_src/getting_started/multiple_brokers/add_broker.py !}
```

!!! note
    The first broker you pass (or add) becomes the application's *default* broker, available as `#!python app.broker`. All registered brokers are available as the `#!python app.brokers` list.

## Testing

Each broker is tested in isolation with its own [`TestBroker`](./subscription/test.md){.internal-link}. Wrap every broker your application uses in the matching test context manager, and the in-memory patches apply across all of them — so the bridge keeps working end to end:

```python linenums="1" hl_lines="3-4 11-14 17-18"
{!> docs_src/getting_started/multiple_brokers/testing.py !}
```

Publishing a message to the Kafka subscriber triggers the bridge, and the message reaches the NATS handler — all without any running broker.

### Same-Type Brokers with a Shared TestBroker

When your application runs several brokers of the **same type** (for example, two **Kafka** clusters), you don't need a separate context manager for each one. Pass them all to a single `TestBroker` instead — it patches every broker you give it and routes messages between them in memory:

```python linenums="1" hl_lines="3 10"
{!> docs_src/getting_started/multiple_brokers/same_type_testing.py !}
```

This is the recommended way to test same-type setups: one `#!python TestKafkaBroker(broker_1, broker_2)` keeps the brokers wired together, so a message bridged from the first cluster is delivered to a subscriber on the second.

!!! note
    Mix both styles freely — use a shared `TestBroker` per broker type, and nest those context managers when your application combines different types (e.g. `#!python TestKafkaBroker(...)` together with `#!python TestNatsBroker(...)`).
