---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Custom Broker

**FastStream** ships brokers for Kafka, RabbitMQ, NATS, Redis, MQTT and Confluent. If you need to talk to a protocol that isn't supported out of the box, you can build your own broker on top of the same base classes those six are built from: `#!python BrokerConfig`, `#!python Registrator` and `#!python BrokerUsecase`, all importable directly from `#!python faststream`.

!!! warning "Advanced / extension API"
    This is an advanced extension point, not a typical application-level feature. Most users will never need it — reach for it only when you're integrating a new transport.

## The Three Building Blocks

### `BrokerConfig`

A `#!python dataclass` holding broker-wide state: middlewares, parser/decoder, the producer, logger, ack policy, and more (see [`faststream/_internal/configs/broker.py`](https://github.com/ag2ai/faststream/blob/main/faststream/_internal/configs/broker.py){.external-link target="_blank"}). Every built-in broker subclasses it to add protocol-specific fields — typically the actual connection object:

```python
from dataclasses import dataclass

from faststream import BrokerConfig


@dataclass(kw_only=True)
class MyBrokerConfig(BrokerConfig):
    connection_url: str
```

### `Registrator`

`#!python Registrator[MsgType, BrokerConfigType]` owns the subscriber/publisher registries shared by both brokers and routers. Its `#!python subscriber()`/`#!python publisher()` methods are the extension point: a subclass builds a concrete subscriber/publisher object for the transport and calls `#!python super().subscriber(...)`/`#!python super().publisher(...)` to register it (see [`faststream/_internal/broker/registrator.py`](https://github.com/ag2ai/faststream/blob/main/faststream/_internal/broker/registrator.py){.external-link target="_blank"}).


#### `create_subscriber` / `create_publisher`

Every built-in broker keeps the "build a concrete endpoint object" step out of `#!python Registrator.subscriber()`/`#!python publisher()` itself and delegates to a pair of module-level factory functions, `#!python create_subscriber` and `#!python create_publisher`. Each protocol defines its own pair — there's no shared, generic factory — living next to the endpoint classes they build, e.g. `faststream/redis/subscriber/factory.py` and `faststream/redis/publisher/factory.py`. They aren't re-exported as public API, so treat them the same way as `#!python SubscriberUsecase`/`#!python PublisherUsecase`: source-level reference, not an importable contract (please notice, that
 `#!python create_publisher` and `#!python create_subscriber` should return these usecases/their subclasses, as they are
 expected in abstract methods of `Registrator`).

A factory takes the endpoint-specific target (Redis's `#!python channel`/`#!python list`/`#!python stream`, MQTT's `#!python topic`), the broker's `#!python config` (so the endpoint can read shared middlewares/logger/etc.), a handful of protocol options (`#!python ack_policy`, `#!python no_reply`, `#!python max_workers` for subscribers; `#!python headers`, delivery options for publishers), and the AsyncAPI trio `#!python title_`/`#!python description_`/`#!python include_in_schema`. It returns the right concrete subscriber/publisher subclass for the options given — for example Redis's `#!python create_subscriber` returns one of `#!python ChannelSubscriber`, `#!python StreamSubscriber`, `#!python ListSubscriber` (or their batch/concurrent variants) depending on which of `#!python channel`/`#!python list`/`#!python stream` was set.

The registrator method itself stays thin — build via the factory, register via `#!python super().subscriber(...)`/`#!python super().publisher(...)`, and (subscribers only) attach the handler and its parser/decoder/codec/dependencies with `#!python .add_call(...)` afterwards:

```python
def subscriber(self, topic: str, /, **options: Any) -> SubscriberUsecase:
    subscriber = create_subscriber(
        topic=topic,
        config=cast(MyBrokerConfig, self.config),
        title_=options.get("title"),
        description_=options.get("description"),
        include_in_schema=options.get("include_in_schema", True),
        # ... protocol-specific options
    )
    super().subscriber(subscriber, persistent=True)
    return subscriber.add_call(
        parser_=options.get("parser") or self._parser,
        decoder_=options.get("decoder") or self._decoder,
        codec_=options.get("codec"),
        dependencies_=options.get("dependencies", ()),
    )
```

### `BrokerUsecase`

`#!python BrokerUsecase[MsgType, ConnectionType, BrokerConfigType]` mixes `#!python Registrator` together with connection lifecycle and publish/request plumbing (`#!python connect()`, `#!python stop()`, the `#!python async with broker:` protocol). A subclass must implement:

- `#!python async def _connect(self) -> ConnectionType` — open the real connection; called once and memoized by `#!python connect()`.
- `#!python async def ping(self, timeout: float | None) -> bool` — health check.
- `#!python async def publish(self, message, queue, /) -> Any` and `#!python async def request(self, message, queue, /, timeout=0.5) -> Any` — build a `#!python PublishCommand` and delegate to the inherited `#!python self._basic_publish(...)` / `#!python self._basic_request(...)` helpers, which already run the middleware stack for you.

```python
from typing import Any

from faststream import BrokerConfig, BrokerUsecase, Registrator


class MyRegistrator(Registrator[Any, MyBrokerConfig]):
    ...  # subscriber() / publisher() — see note above


class MyBroker(MyRegistrator, BrokerUsecase[Any, MyBrokerConfig]):
    async def _connect(self) -> Any:
        connection = await open_my_connection(self.config.connection_url)
        return connection

    async def ping(self, timeout: float | None) -> bool:
        return self._connection is not None

    async def publish(self, message, queue, /) -> Any:
        cmd = build_publish_command(message, queue)
        return await self._basic_publish(cmd, producer=self._producer)

    async def request(self, message, queue, /, timeout=0.5) -> Any:
        cmd = build_publish_command(message, queue, timeout=timeout)
        return await self._basic_request(cmd, producer=self._producer)
```

`#!python open_my_connection` and `#!python build_publish_command` stand in for real transport code — there's no generic implementation for either, since they're entirely protocol-specific.

## A Complete Reference Implementation

The clearest way to see all three pieces wired together is to read a built-in broker end to end. **Redis** is the smallest complete example:

- [`faststream/redis/configs/broker.py`](https://github.com/ag2ai/faststream/blob/main/faststream/redis/configs/broker.py){.external-link target="_blank"} — `#!python RedisBrokerConfig`, a `#!python BrokerConfig` subclass carrying the connection state, producer and message format, plus `#!python connect()`/`#!python disconnect()` coroutines.
- [`faststream/redis/broker/registrator.py`](https://github.com/ag2ai/faststream/blob/main/faststream/redis/broker/registrator.py){.external-link target="_blank"} — `#!python RedisRegistrator`, implementing `#!python subscriber()`/`#!python publisher()` for channels, lists and streams.
- [`faststream/redis/subscriber/factory.py`](https://github.com/ag2ai/faststream/blob/main/faststream/redis/subscriber/factory.py){.external-link target="_blank"} and [`faststream/redis/publisher/factory.py`](https://github.com/ag2ai/faststream/blob/main/faststream/redis/publisher/factory.py){.external-link target="_blank"} — `#!python create_subscriber`/`#!python create_publisher`, the factories `#!python RedisRegistrator` calls into.
- [`faststream/redis/broker/broker.py`](https://github.com/ag2ai/faststream/blob/main/faststream/redis/broker/broker.py){.external-link target="_blank"} — `#!python RedisBroker`, implementing `#!python _connect()`, `#!python ping()`, `#!python publish()`, `#!python request()` and the `#!python start()`/`#!python stop()` overrides.

For a subscriber/publisher consume loop specifically, [`faststream/mqtt/subscriber/usecase.py`](https://github.com/ag2ai/faststream/blob/main/faststream/mqtt/subscriber/usecase.py){.external-link target="_blank"} is the shortest of the six and a good template to follow.

!!! note "Out of scope here"
    This page covers the broker wiring itself. AsyncAPI schema generation and [`TestBroker`](../subscription/test.md){.internal-link} in-memory testing support are separate layers built on top of a broker — replicating them for a custom broker means implementing the corresponding specification and testing base classes found alongside each built-in broker (e.g. `faststream/redis/testing.py`).
