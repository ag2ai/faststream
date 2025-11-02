# FastStream

<b>Effortless event stream integration for your services</b>

---

<div align="center">

[![Trendshift](https://trendshift.io/api/badge/repositories/3043)](https://trendshift.io/repositories/3043)

[![Test Passing](https://github.com/ag2ai/faststream/actions/workflows/pr_tests.yaml/badge.svg?branch=main)](https://github.com/ag2ai/faststream/actions/workflows/pr_tests.yaml)
[![Coverage](https://coverage-badge.samuelcolvin.workers.dev/ag2ai/faststream.svg)](https://coverage-badge.samuelcolvin.workers.dev/redirect/ag2ai/faststream)
[![Downloads](https://static.pepy.tech/personalized-badge/faststream?period=month&units=international_system&left_color=grey&right_color=green&left_text=downloads/month)](https://www.pepy.tech/projects/faststream)
[![Package version](https://img.shields.io/pypi/v/faststream?label=PyPI)](https://pypi.org/project/faststream)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/faststream.svg)](https://pypi.org/project/faststream)\
[![CodeQL](https://github.com/ag2ai/faststream/actions/workflows/pr_codeql.yaml/badge.svg)](https://github.com/ag2ai/faststream/actions/workflows/pr_codeql.yaml)
[![Dependency Review](https://github.com/ag2ai/faststream/actions/workflows/pr_dependency-review.yaml/badge.svg)](https://github.com/ag2ai/faststream/actions/workflows/pr_dependency-review.yaml)
[![License](https://img.shields.io/github/license/ag2ai/faststream.svg)](https://github.com/ag2ai/faststream/blob/main/LICENSE)
[![Code of Conduct](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](https://github.com/ag2ai/faststream/blob/main/.github/CODE_OF_CONDUCT.md)\
[![Discord](https://img.shields.io/discord/1085457301214855171?logo=discord&label=EN)](https://discord.gg/qFm6aSqq59)
[![FastStream](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fag2ai%2Ffaststream%2Fmain%2Fdocs%2Fdocs%2Fassets%2Fimg%2Fshield.json)](https://faststream.ag2.ai)
[![Telegram](https://img.shields.io/badge/-telegram-black?color=blue&logo=telegram&label=RU)](https://t.me/python_faststream)\
[![Gurubase](https://img.shields.io/badge/Gurubase-Ask%20FastStream%20Guru-006BFF)](https://gurubase.io/g/faststream)
</div>

---

## Features

[**FastStream**](https://faststream.ag2.ai/latest/) simplifies the process of writing producers and consumers for message queues, handling all the
parsing, networking and documentation generation automatically.

Making streaming microservices has never been easier. Designed with junior developers in mind, **FastStream** simplifies your work while keeping the door open for more advanced use cases. Here's a look at the core features that make **FastStream** a go-to framework for modern, data-centric microservices.

- [**Multiple Brokers**](#unified-api): **FastStream** provides a suitable API to work across multiple message brokers ([**Kafka**](https://kafka.apache.org/), [**RabbitMQ**](https://www.rabbitmq.com/), [**NATS**](https://nats.io/), [**Redis**](https://redis.io/) support)

- [**Built-in Serialization**](#writing-app-code): Leverage [**Pydantic**](https://docs.pydantic.dev/) or [**Msgspec**](https://jcristharif.com/msgspec/) validation capabilities to serialize and validate incoming messages

- [**Automatic Docs**](#project-documentation): Stay ahead with automatic [**AsyncAPI**](https://www.asyncapi.com/) documentation

- **Intuitive**: Full-typed editor support makes your development experience smooth, catching errors before they reach runtime

- [**Powerful Dependency Injection System**](#dependencies): Manage your service dependencies efficiently with **FastStream**'s built-in DI system

- [**Testable**](#testing-the-service): Supports in-memory tests, making your CI/CD pipeline faster and more reliable

- **Extensible**: Use extensions for lifespans, custom serialization and middleware

- [**Integrations**](#any-framework): **FastStream** is fully compatible with any HTTP framework you want ([**FastAPI**](#fastapi-plugin) especially)

That's **FastStream** in a nutshell - easy, efficient, and powerful. Whether you're just starting with streaming microservices or looking to scale, **FastStream** has got you covered.

---

**Documentation**: <a href="https://faststream.ag2.ai/latest/" target="_blank">https://faststream.ag2.ai/latest/</a>

<details>
<summary><b>Table of Contents</b></summary>

- [Features](#features)
- [Versioning Policy](#versioning-policy)
- [Installation](#installation)
- [Quick Start](#writing-app-code)
  - [Pydantic serialization](#pydantic-serialization)
  - [Msgspec serialization](#msgspec-serialization)
  - [Unified API](#unified-api)
- [Testing](#testing-the-service)
- [CLI](#running-the-application)
- [AsyncAPI Documentation](#project-documentation)
- [Dependencies](#dependencies)
- [Integrations](#http-frameworks-integrations)
  - [Any Framework](#any-framework)
  - [**FastAPI** Plugin](#fastapi-plugin)
- [Stay in touch](#stay-in-touch)

</details>

<details>
<summary><b>Project History</b></summary>

**FastStream** is a package based on the ideas and experiences gained from [**FastKafka**](https://github.com/airtai/fastkafka) and [**Propan**](https://github.com/lancetnik/propan). By joining our forces, we picked up the best from both packages and created a unified way to write services capable of processing streamed data regardless of the underlying protocol.
</details>

---

## Versioning Policy

FastStream has a stable public API. Only major updates may introduce breaking changes.

Prior to FastStream's 1.0 release, each minor update is considered a major and can introduce breaking changes, but these changes were communicated through two-versions deprecation warnings prior to being fully removed. So features deprecated in the 0.4 version were only removed in version 0.6.

Our team is working toward the stable 1.0 version.

## Installation

**FastStream** works on **Linux**, **macOS**, **Windows** and most **Unix**-style operating systems.
You can install it with `pip` as usual:

```sh
pip install 'faststream[kafka]'
# or
pip install 'faststream[confluent]'
# or
pip install 'faststream[rabbit]'
# or
pip install 'faststream[nats]'
# or
pip install 'faststream[redis]'
```

---

## Writing app code

**FastStream** brokers provide convenient function decorators `@broker.subscriber`
and `@broker.publisher` to allow you to delegate the actual process of:

- consuming and producing data to Event queues, and

- decoding and encoding JSON-encoded messages

These decorators make it easy to specify the processing logic for your consumers and producers, allowing you to focus on the core business logic of your application without worrying about the underlying integration.

Also, **FastStream** uses [**Pydantic**](https://docs.pydantic.dev/) to parse input
JSON-encoded data into Python objects, making it easy to work with structured data in your applications, so you can serialize your input messages just using type annotations.

Here is an example Python app using **FastStream** that consumes data from an incoming data stream and outputs the data to another one:

```python
from faststream import FastStream
from faststream.kafka import KafkaBroker
# from faststream.confluent import KafkaBroker
# from faststream.rabbit import RabbitBroker
# from faststream.nats import NatsBroker
# from faststream.redis import RedisBroker

broker = KafkaBroker("localhost:9092")
# broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
# broker = NatsBroker("nats://localhost:4222/")
# broker = RedisBroker("redis://localhost:6379/")

app = FastStream(broker)

@broker.subscriber("in")
@broker.publisher("out")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
```

### Pydantic serialization

Also, **Pydantic**’s [`BaseModel`](https://docs.pydantic.dev/usage/models/) class allows you
to define messages using a declarative syntax, making it easy to specify the fields and types of your messages.

```python
from pydantic import BaseModel, Field, PositiveInt
from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

class User(BaseModel):
    user: str = Field(..., examples=["John"])
    user_id: PositiveInt = Field(..., examples=["1"])

@broker.subscriber("in")
@broker.publisher("out")
async def handle_msg(data: User) -> str:
    return f"User: {data.user} - {data.user_id} registered"
```

By default we use **PydanticV2** written in **Rust** as serialization library, but you can downgrade it manually, if your platform has no **Rust** support - **FastStream** will work correctly with **PydanticV1** as well.

To choose the **Pydantic** version, you can install the required one using the regular

```shell
pip install pydantic==1.X.Y
```

**FastStream** (and **FastDepends** inside) should work correctly with almost any version.

### Msgspec serialization

Moreover, **FastStream** is not tied to any specific serialization library, so you can use any preferred one. Fortunately, we provide a built‑in alternative for the most popular **Pydantic** replacement - [**Msgspec**](https://jcristharif.com/msgspec/).

```python
from fast_depends.msgspec import MsgSpecSerializer
from faststream.kafka import KafkaBroker

broker = KafkaBroker(serializer=MsgSpecSerializer())
```

You can read more about the feature in the [documentation](https://faststream.ag2.ai/latest/gettings-started/subscription/msgspec/).

### Unified API

At first glance, **FastStream** unifies various broker backends under a single API. However, a completely unified API inevitably results in missing features. We do not want to limit users' choices. If you prefer Kafka over Redis, there is a reason. Therefore, we support all native broker features you need.

Consequently, our unified API has a relatively limited scope:

```python
from faststream.[broker] import [Broker], [Broker]Message

broker = [Broker](*servers)

@broker.subscriber([source])  # Kafka topic / RMQ queue / NATS subject / etc
@broker.publisher([destination])  # topic / routing key / subject / etc
async def handler(msg: [Broker]Message) -> None:
    await msg.ack()  # control brokers' acknowledgement policy

...

await broker.publish("Message", [destiination])
```

Beyond this scope you can use any broker-native features you need:

* **Kafka** - specific partition reads, partitioner control, consumer groups, batch processing, etc.
* **RabbitMQ** - all exchange types, Redis Streams, RPC, manual channel configuration, DLQ, etc.
* **NATS** - core and Push/Pull JetStream subscribers, KeyValue, ObjectStorage, RPC, etc.
* **Redis** - Pub/Sub, List, Stream subscribers, consumer groups, acknowledgements, etc.

You can find detailed information about all supported features in **FastStream**’s broker‑specific documentation.

If a particular feature is missing or not yet supported, you can always fall back to the native broker client/connection for those operations.

---

## Testing the service

The service can be tested using the `TestBroker` context managers, which, by default, puts the Broker into "testing mode".

The Tester will redirect your `subscriber` and `publisher` decorated functions to the InMemory brokers, allowing you to quickly test your app without the need for a running broker and all its dependencies.

Using pytest, the test for our service would look like this:

```python

import pytest
import pydantic
from faststream.kafka import TestKafkaBroker


@pytest.mark.asyncio
async def test_correct():
    async with TestKafkaBroker(broker) as br:
        await br.publish({
            "user": "John",
            "user_id": 1,
        }, "in")

@pytest.mark.asyncio
async def test_invalid():
    async with TestKafkaBroker(broker) as br:
        with pytest.raises(pydantic.ValidationError):
            await br.publish("wrong message", "in")
```

---

## Running the application

The application can be started using built-in **FastStream** CLI command.

Before running the service, install **FastStream CLI** using the following command:

```shell
pip install "faststream[cli]"
```

To run the service, use the **FastStream CLI** command and pass the module (in this case, the file where the app implementation is located) and the app symbol to the command.

```shell
faststream run basic:app
```

After running the command, you should see the following output:

```shell
INFO     - FastStream app starting...
INFO     - input_data |            - `HandleMsg` waiting for messages
INFO     - FastStream app started successfully! To exit press CTRL+C
```

Also, **FastStream** provides you with a great hot reload feature to improve your Development Experience

```shell
faststream run basic:app --reload
```

And multiprocessing horizontal scaling feature as well:

```shell
faststream run basic:app --workers 3
```

You can learn more about **CLI** features [here](https://faststream.ag2.ai/latest/getting-started/cli/)

---

## Project Documentation

**FastStream** automatically generates documentation for your project according to the [**AsyncAPI**](https://www.asyncapi.com/) specification. You can work with both generated artifacts and place a web view of your documentation on resources available to related teams.

The availability of such documentation significantly simplifies the integration of services: you can immediately see what channels and message formats the application works with. And most importantly, it won't cost anything - **FastStream** has already created the docs for you!

![HTML-page](https://github.com/ag2ai/faststream/blob/main/docs/docs/assets/img/AsyncAPI-basic-html-short.png?raw=true)

---

## Dependencies

**FastStream** (thanks to [**FastDepends**](https://lancetnik.github.io/FastDepends/)) has a dependency management system similar to `pytest fixtures` and `FastAPI Depends` at the same time. Function arguments declare which dependencies you want are needed, and a special decorator delivers them from the global Context object.

```python
from typing import Annotated
from faststream import Depends, Logger

async def base_dep(user_id: int) -> bool:
    return True

@broker.subscriber("in-test")
async def base_handler(user: str,
                       logger: Logger,
                       dep: Annotated[bool, Depends(base_dep)]):
    assert dep is True
    logger.info(user)
```

---

## HTTP Frameworks integrations

### Any Framework

You can use **FastStream** `MQBrokers` without a `FastStream` application.
Just *start* and *stop* them according to your application's lifespan.

```python
from aiohttp import web

from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")

@broker.subscriber("test")
async def base_handler(body):
    print(body)

async def start_broker(app):
    await broker.start()

async def stop_broker(app):
    await broker.stop()

async def hello(request):
    return web.Response(text="Hello, world")

app = web.Application()
app.add_routes([web.get("/", hello)])
app.on_startup.append(start_broker)
app.on_cleanup.append(stop_broker)

if __name__ == "__main__":
    web.run_app(app)
```

### **FastAPI** Plugin

Also, **FastStream** can be used as part of **FastAPI**.

Just import a **StreamRouter** you need and declare the message handler with the same `@router.subscriber(...)` and `@router.publisher(...)` decorators.

```python
from fastapi import FastAPI
from pydantic import BaseModel

from faststream.kafka.fastapi import KafkaRouter

router = KafkaRouter("localhost:9092")

class Incoming(BaseModel):
    m: dict

@router.subscriber("test")
@router.publisher("response")
async def hello(m: Incoming):
    return {"response": "Hello, world!"}

app = FastAPI()
app.include_router(router)
```

More integration features can be found [here](https://faststream.ag2.ai/latest/getting-started/integrations/fastapi/)

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
