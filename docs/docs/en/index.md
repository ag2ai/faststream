---
hide:
  - navigation
search:
  exclude: true
---

# FastStream

**FastStream** is an asynchronous Python framework for building event-driven applications.

If you know [**FastAPI**](https://fastapi.tiangolo.com/){.external-link target="_blank"}, you already know **FastStream**: the same decorators, type-driven validation, dependency injection and generated documentation — pointed at Kafka, RabbitMQ, NATS, Redis and MQTT instead of HTTP. It takes the boilerplate off your hands and leaves your broker intact.

---

<p align="center">
  <a href="https://trendshift.io/repositories/19979" target="_blank"><img src="https://trendshift.io/api/badge/repositories/19979" alt="ag2ai%2Ffaststream | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

  <br/>
  <br/>

  <a href="https://github.com/ag2ai/faststream/actions/workflows/pr_tests.yaml" target="_blank">
    <img src="https://github.com/ag2ai/faststream/actions/workflows/pr_tests.yaml/badge.svg?branch=main" alt="Test Passing"/>
  </a>

  <a href="https://coverage-badge.samuelcolvin.workers.dev/redirect/ag2ai/faststream" target="_blank">
      <img src="https://coverage-badge.samuelcolvin.workers.dev/ag2ai/faststream.svg" alt="Coverage"/>
  </a>

  <a href="https://www.pepy.tech/projects/faststream" target="_blank">
    <img src="https://static.pepy.tech/personalized-badge/faststream?period=month&units=international_system&left_color=grey&right_color=green&left_text=downloads/month" alt="Downloads"/>
  </a>

  <a href="https://pypi.org/project/faststream" target="_blank">
    <img src="https://img.shields.io/pypi/v/faststream?label=PyPI" alt="Package version">
  </a>

  <a href="https://pypi.org/project/faststream" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/faststream.svg" alt="Supported Python versions">
  </a>

  <br/>

  <a href="https://github.com/ag2ai/faststream/actions/workflows/pr_codeql.yaml" target="_blank">
    <img src="https://github.com/ag2ai/faststream/actions/workflows/pr_codeql.yaml/badge.svg" alt="CodeQL"/>
  </a>

  <a href="https://github.com/ag2ai/faststream/actions/workflows/pr_dependency-review.yaml" target="_blank">
    <img src="https://github.com/ag2ai/faststream/actions/workflows/pr_dependency-review.yaml/badge.svg" alt="Dependency Review"/>
  </a>

  <a href="https://github.com/ag2ai/faststream/blob/main/LICENSE" target="_blank">
    <img src="https://img.shields.io/github/license/ag2ai/faststream.svg" alt="License"/>
  </a>

  <a href="https://github.com/ag2ai/faststream/blob/main/CODE_OF_CONDUCT.md" target="_blank">
    <img src="https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg" alt="Code of Conduct"/>
  </a>

  <br/>

  <a href="https://discord.gg/qFm6aSqq59" target="_blank">
      <img alt="Discord" src="https://img.shields.io/discord/1085457301214855171?logo=discord&label=EN"/>
  </a>

  <img src="https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fag2ai%2Ffaststream%2Fmain%2Fdocs%2Fdocs%2Fassets%2Fimg%2Fshield.json" alt="FastStream"/>

  <a href="https://t.me/python_faststream" target="_blank">
    <img alt="Telegram" src="https://img.shields.io/badge/-telegram-black?color=blue&logo=telegram&label=RU"/>
  </a>

  <br/>

  <a href="https://gurubase.io/g/faststream" target="_blank">
    <img alt="Gurubase" src="https://img.shields.io/badge/Gurubase-Ask%20FastStream%20Guru-006BFF"/>
  </a>
</p>

---

## Features

[**FastStream**](https://faststream.ag2.ai/) simplifies the process of writing producers and consumers for message queues, handling all the
parsing, lifecycle and documentation generation automatically.

Making streaming microservices has never been easier. The API is small enough to onboard a teammate in an afternoon, and it never costs you access to the broker underneath — approachable and complete are not a trade-off here. Here's a look at the core features that make **FastStream** a go-to framework for modern, data-centric microservices.

- [**A Spec You Never Write**](#project-documentation): a full [**AsyncAPI**](https://www.asyncapi.com/){.external-link target="_blank"} document generated from your handlers — the contract the neighbouring team keeps asking for, guaranteed to match the code, with an in-browser form for publishing test messages

- [**Tests Without a Broker**](#testing-the-service): an in-memory test client runs your subscribers and publishers with validation intact — no containers in CI, no flakes, milliseconds instead of minutes

- [**Observable From Day One**](https://faststream.ag2.ai/latest/getting-started/observability/opentelemetry/){.internal-link}: OpenTelemetry traces, Prometheus metrics and Kubernetes probes come with the framework — a couple of middlewares instead of a few hundred lines in every service

- [**Your Broker, In Full**](#your-broker-in-full): **FastStream** is a client for *your* broker, not a layer above all of them — [**Kafka**](https://kafka.apache.org/){.external-link target="_blank"} consumer groups and partitioning, [**RabbitMQ**](https://www.rabbitmq.com/){.external-link target="_blank"} exchanges and DLQ, [**NATS**](https://nats.io/){.external-link target="_blank"} JetStream and KeyValue, [**Redis**](https://redis.io/){.external-link target="_blank"} Streams, [**MQTT**](https://mqtt.org/){.external-link target="_blank"} QoS. Five first-class clients that happen to share their ergonomics.

- [**Built-in Serialization**](#writing-app-code): Leverage [**Pydantic**](https://docs.pydantic.dev/){.external-link target="_blank"} or [**Msgspec**](https://jcristharif.com/msgspec/){.external-link target="_blank"} validation capabilities to serialize and validate incoming messages

- [**Powerful Dependency Injection System**](#dependencies): Manage your service dependencies efficiently with **FastStream**'s built-in DI system

- **Intuitive**: Full-typed editor support makes your development experience smooth, catching errors before they reach runtime

- **Extensible**: Use extensions for lifespans, custom serialization and middleware

- [**Integrations**](#any-framework): **FastStream** is fully compatible with any HTTP framework you want — including a dedicated [**FastAPI** plugin](#fastapi-plugin-deprecated), now shipped as its own package

That is **FastStream**: everything a messaging service needs around your handlers, and nothing between you and your broker.

??? info "Project History"
    **FastStream** is a package based on the ideas and experiences gained from [**FastKafka**](https://github.com/airtai/fastkafka){.external-link target="_blank"} and [**Propan**](https://github.com/lancetnik/propan){.external-link target="_blank"}. By joining our forces, we picked up the best from both packages and created a unified way to write services capable of processing streamed data regardless of the underlying protocol.

<a id="versioning-policy"></a>

??? info "Versioning Policy"
    FastStream has a stable public API. Only major updates may introduce breaking changes.

    Prior to FastStream's 1.0 release, each minor update is considered a major and can introduce breaking changes, but these changes were communicated through two-versions deprecation warnings prior to being fully removed. So features deprecated in the 0.4 version were only removed in version 0.6.

    Our team is working toward the stable 1.0 version.

---

## Installation

**FastStream** works on **Linux**, **macOS**, **Windows** and most **Unix**-style operating systems.
You can install it with `pip` as usual:

=== "AIOKafka"
    ```sh
    pip install 'faststream[kafka]'
    ```

=== "Confluent"
    ```sh
    pip install 'faststream[confluent]'
    ```

=== "RabbitMQ"
    ```sh
    pip install 'faststream[rabbit]'
    ```

=== "NATS"
    ```sh
    pip install 'faststream[nats]'
    ```

=== "Redis"
    ```sh
    pip install 'faststream[redis]'
    ```

=== "MQTT"
    ```sh
    pip install 'faststream[mqtt]'
    ```

---

## Writing app code

**FastStream** brokers provide convenient function decorators `#!python @broker.subscriber(...)`
and `#!python @broker.publisher(...)` to allow you to delegate the actual process of:

- consuming and producing data to Event queues, and

- decoding and encoding JSON-encoded messages

These decorators make it easy to specify the processing logic for your consumers and producers, allowing you to focus on the core business logic of your application without worrying about the underlying integration.

Also, **FastStream** uses [**Pydantic**](https://docs.pydantic.dev/){.external-link target="_blank"} to parse input
JSON-encoded data into Python objects, making it easy to work with structured data in your applications, so you can serialize your input messages just using type annotations.

Here is an example Python app using **FastStream** that consumes data from an incoming data stream and outputs the data to another one:

=== "AIOKafka"
    ```python linenums="1" hl_lines="2 4"
    {!> docs_src/index/kafka/basic.py!}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="2 4"
    {!> docs_src/index/confluent/basic.py!}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="2 4"
    {!> docs_src/index/rabbit/basic.py!}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="2 4"
    {!> docs_src/index/nats/basic.py!}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="2 4"
    {!> docs_src/index/redis/basic.py!}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="2 4"
    {!> docs_src/index/mqtt/basic.py!}
    ```

### Pydantic serialization

Also, **Pydantic**’s [`BaseModel`](https://docs.pydantic.dev/usage/models/){.external-link target="_blank"} class allows you
to define messages using a declarative syntax, making it easy to specify the fields and types of your messages.

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 8 14"
    {!> docs_src/index/kafka/pydantic.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 8 14"
    {!> docs_src/index/confluent/pydantic.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 8 14"
    {!> docs_src/index/rabbit/pydantic.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 8 14"
    {!> docs_src/index/nats/pydantic.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 8 14"
    {!> docs_src/index/redis/pydantic.py !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="1 8 14"
    {!> docs_src/index/mqtt/pydantic.py !}
    ```

!!! tip ""
    By default we use **PydanticV2** written in **Rust** as serialization library, but you can downgrade it manually, if your platform has no **Rust** support - **FastStream** will work correctly with **PydanticV1** as well.

    To choose the **Pydantic** version, you can install the required one using the regular

    ```shell
    pip install pydantic==1.X.Y
    ```

    **FastStream** (and **FastDepends** inside) should work correctly with almost any version.


### Msgspec serialization

Moreover, **FastStream** is not tied to any specific serialization library, so you can use any preferred one. Fortunately, we provide a built‑in alternative for the most popular **Pydantic** replacement - [**Msgspec**](https://jcristharif.com/msgspec/){.external-link target="_blank"}.

=== "AIOKafka"
    ```python linenums="1" hl_lines="1 4"
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.kafka import KafkaBroker

    broker = KafkaBroker(serializer=MsgSpecSerializer())
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="1 4"
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.confluent import KafkaBroker

    broker = KafkaBroker(serializer=MsgSpecSerializer())
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="1 4"
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.rabbit import RabbitBroker

    broker = RabbitBroker(serializer=MsgSpecSerializer())
    ```

=== "NATS"
    ```python linenums="1" hl_lines="1 4"
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.nats import NatsBroker

    broker = NatsBroker(serializer=MsgSpecSerializer())
    ```

=== "Redis"
    ```python linenums="1" hl_lines="1 4"
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.redis import RedisBroker

    broker = RedisBroker(serializer=MsgSpecSerializer())
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="1 4"
    from fast_depends.msgspec import MsgSpecSerializer
    from faststream.mqtt import MQTTBroker

    broker = MQTTBroker("localhost", port=1883, serializer=MsgSpecSerializer())
    ```

You can read more about the feature in the [documentation](https://faststream.ag2.ai/latest/getting-started/subscription/msgspec/){.internal-link}.


<a id="unified-api"></a>

### Your Broker, In Full

**FastStream is a thin client, not an abstraction layer.** It wraps your broker's own library — `aiokafka` or `confluent-kafka`, `aio-pika`, `nats-py`, `redis-py`, `zmqtt` — and takes over what every service otherwise rewrites by hand: lifecycle, serialization, acknowledgement, observability, documentation, tests. What the broker itself offers stays yours.

Two rules follow, and they explain most of our API decisions:

1. **We do not implement business logic.** No retries, no delayed delivery, no task orchestration. Those are architectural choices, and a framework that makes them for you owns your architecture.
2. **Every native broker feature stays reachable.** When the ergonomic path is not enough, the client underneath is one annotation away — `Connection` and `Channel` for RabbitMQ, `Consumer` for Kafka, `Client` for NATS and MQTT, `Redis` for Redis. Every broker we support has one.

What the five clients share is a deliberately small surface:

=== "AIOKafka"
    ```python linenums="1"
    from faststream.kafka import KafkaBroker, KafkaMessage

    broker = KafkaBroker("localhost:9092")

    @broker.subscriber("in-topic")
    @broker.publisher("out-topic")
    async def handler(msg: KafkaMessage) -> None:
        await msg.ack()  # control brokers' acknowledgement policy

    ...

    await broker.publish("Message", "in-topic")
    ```

=== "Confluent"
    ```python linenums="1"
    from faststream.confluent import KafkaBroker, KafkaMessage

    broker = KafkaBroker("localhost:9092")

    @broker.subscriber("in-topic")
    @broker.publisher("out-topic")
    async def handler(msg: KafkaMessage) -> None:
        await msg.ack()  # control brokers' acknowledgement policy

    ...

    await broker.publish("Message", "in-topic")
    ```

=== "RabbitMQ"
    ```python linenums="1"
    from faststream.rabbit import RabbitBroker, RabbitMessage

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

    @broker.subscriber("in-queue")
    @broker.publisher("out-queue")
    async def handler(msg: RabbitMessage) -> None:
        await msg.ack()  # control brokers' acknowledgement policy

    ...

    await broker.publish("Message", "in-queue")
    ```

=== "NATS"
    ```python linenums="1"
    from faststream.nats import NatsBroker, NatsMessage

    broker = NatsBroker("nats://localhost:4222")

    @broker.subscriber("in-subject")
    @broker.publisher("out-subject")
    async def handler(msg: NatsMessage) -> None:
        await msg.ack()  # control brokers' acknowledgement policy

    ...

    await broker.publish("Message", "in-subject")
    ```

=== "Redis"
    ```python linenums="1"
    from faststream.redis import RedisBroker, RedisMessage

    broker = RedisBroker("redis://localhost:6379")

    @broker.subscriber("in-channel")
    @broker.publisher("out-channel")
    async def handler(msg: RedisMessage) -> None:
        await msg.ack()  # control brokers' acknowledgement policy

    ...

    await broker.publish("Message", "in-channel")
    ```

=== "MQTT"
    ```python linenums="1"
    from faststream.mqtt import MQTTBroker, MQTTMessage

    broker = MQTTBroker("localhost", port=1883)

    @broker.subscriber("in-topic")
    @broker.publisher("out-topic")
    async def handler(msg: MQTTMessage) -> None:
        await msg.ack()  # control brokers' acknowledgement policy

    ...

    await broker.publish("Message", "in-topic")
    ```

Beyond this scope you can use any broker-native features you need:

* **Kafka** - specific partition reads, partitioner control, consumer groups, batch processing, etc.
* **RabbitMQ** - all exchange types, Redis Streams, RPC, manual channel configuration, DLQ, etc.
* **NATS** - core and Push/Pull JetStream subscribers, KeyValue, ObjectStorage, RPC, etc.
* **Redis** - Pub/Sub, List, Stream subscribers, consumer groups, acknowledgements, etc.
* **MQTT** - topic subscriptions (including wildcards), QoS and retain, MQTT 3.1.1 and 5.0, request/reply (RPC), TLS, etc.

You can find detailed information about all supported features in **FastStream**’s broker‑specific documentation.

If a particular feature is missing or not yet supported, you can always fall back to the native broker client/connection for those operations.

---
## Testing the service

The service can be [tested](./getting-started/subscription/test.md){.internal-link} using the `TestBroker` context managers, which, by default, puts the Broker into "testing mode".

The Tester will redirect your `subscriber` and `publisher` decorated functions to the InMemory brokers, allowing you to quickly test your app without the need for a running broker and all its dependencies.

Using pytest, the test for our service would look like this:

=== "AIOKafka"
    ```python linenums="1" hl_lines="3 8 18-19"
    {!> docs_src/index/kafka/test.py [ln:3-22] !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3 8 18-19"
    {!> docs_src/index/confluent/test.py [ln:3-22] !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3 8 18-19"
    {!> docs_src/index/rabbit/test.py [ln:3-22] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3 8 18-19"
    {!> docs_src/index/nats/test.py [ln:3-22] !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3 8 18-19"
    {!> docs_src/index/redis/test.py [ln:3-22] !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="3 8 18-19"
    {!> docs_src/index/mqtt/test.py [ln:3-22] !}
    ```


---

## Running the application

The application can be started using the built-in **FastStream** CLI command.

!!! note
    Before running the service, install **FastStream CLI** using the following command:
    ```shell
    pip install "faststream[cli]"
    ```

To run the service, use the **FastStream CLI** command and pass the module (in this case, the file where the app implementation is located) and the app symbol to the command.

```shell
faststream run basic:app
```

After running the command, you should see the following output:

```{.shell .no-copy}
INFO     - FastStream app starting...
INFO     - input_data |            - `HandleMsg` waiting for messages
INFO     - FastStream app started successfully! To exit press CTRL+C
```
{ data-search-exclude }

Also, **FastStream** provides you with a great hot reload feature to improve your Development Experience

```shell
faststream run basic:app --reload
```

And multiprocessing horizontal scaling feature as well:

```shell
faststream run basic:app --workers 3
```

You can learn more about **CLI** features [here](./getting-started/cli.md){.internal-link}

---

## Project Documentation

**FastStream** automatically generates documentation for your project according to the [**AsyncAPI**](https://www.asyncapi.com/){.external-link target="_blank"} specification. You can work with both generated artifacts and place a web view of your documentation on resources available to related teams.

The availability of such documentation significantly simplifies the integration of services: you can immediately see what channels and message formats the application works with. And most importantly, it won't cost anything - **FastStream** has already created the docs for you!

![HTML-page](../assets/img/AsyncAPI-basic-html-short.png){ .on-glb loading=lazy }

---

## Dependencies

**FastStream** (thanks to [**FastDepends**](https://lancetnik.github.io/FastDepends/){.external-link target="_blank"}) has a dependency management system similar to [`pytest fixtures`](https://docs.pytest.org/en/latest/explanation/fixtures.html){.external-link target="_blank"} and [`FastAPI Depends`](https://fastapi.tiangolo.com/tutorial/dependencies/){.external-link target="_blank"} at the same time. Function arguments declare which dependencies are needed, and a special decorator delivers them from the global Context object.

=== "Non-Annotated"
    ```python
    {!> docs_src/index/dependencies.py !}
    ```

=== "Annotated"
    ```python
    {!> docs_src/index/dependencies_annotated.py !}
    ```

---

## HTTP Frameworks integrations

### Any Framework

You can use **FastStream** `MQBrokers` without a `FastStream` application.
Just *start* and *stop* them according to your application's lifespan.

=== "Litestar"
    ```python linenums="1" hl_lines="2 4 16 17"
    {!> docs_src/integrations/http_frameworks_integrations/litestar.py !}
    ```

=== "Aiohttp"
    ```python linenums="1" hl_lines="3 5 8-10 13-14 17-18 27-28"
    {!> docs_src/integrations/http_frameworks_integrations/aiohttp.py !}
    ```

=== "Blacksheep"
    ```python linenums="1" hl_lines="3 5 10-12 15-17 20-22"
    {!> docs_src/integrations/http_frameworks_integrations/blacksheep.py !}
    ```

=== "Falcon"
    ```python linenums="1" hl_lines="4 6 9-11 26-31 35"
    {!> docs_src/integrations/http_frameworks_integrations/falcon.py !}
    ```

=== "Quart"
    ```python linenums="1" hl_lines="3 5 10-12 15-17 20-22"
    {!> docs_src/integrations/http_frameworks_integrations/quart.py !}
    ```

=== "Sanic"
    ```python linenums="1" hl_lines="4 6 11-13 16-18 21-23"
    {!> docs_src/integrations/http_frameworks_integrations/sanic.py !}
    ```

### **FastAPI** Plugin (deprecated)

!!! warning "Plugin deprecated"
    The integration has been moved to the
    **[faststream_fastapi](https://github.com/faststream-community/faststream_fastapi)**
    package and will be removed in the 1.0.0 version.

    ```bash
    pip install faststream_fastapi
    ```

Also, **FastStream** can be used as part of **FastAPI**.

Just import a **StreamRouter** you need and declare the message handler with the same `#!python @router.subscriber(...)` and `#!python @router.publisher(...)` decorators.

=== "AIOKafka"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/kafka/base.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/confluent/base.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/rabbit/base.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/nats/base.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/redis/base.py !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="4 6 14-18 24-25"
    {!> docs_src/integrations/fastapi/mqtt/base.py !}
    ```

!!! note
    More integration features can be found [here](./getting-started/integrations/fastapi/index.md){.internal-link}

---

## Benchmarks

We use codspeed to run benchmarks for both FastStream itself and raw clients.

---

## Used By

**FastStream** is used by research institutions, public sector organizations and companies — among
them **ECMWF**, **Hydro-Québec**, the **Rubin Observatory**, **NERSC** and **Red Hat**. Neighbouring
projects such as **Pydantic Logfire**, **RabbitMQ** and **EMQX** maintain a **FastStream**
integration of their own.

See the full list on the [Used By](./who-uses.md){.internal-link} page, and open a pull request to add your own project.

---

## Stay in touch

Please show your support and stay in touch by:

- giving our [GitHub repository](https://github.com/ag2ai/faststream/) a star, and

- joining our [EN Discord server](https://discord.gg/qFm6aSqq59)

- joining our [RU Telegram group](https://t.me/python_faststream)

Your support helps us to stay in touch with you and encourages us to
continue developing and improving the framework. Thank you for your
support!

---

## Contributors

Thanks to all of these amazing people who made the project better!

<a href="https://github.com/ag2ai/faststream/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=ag2ai/faststream"/>
</a>
