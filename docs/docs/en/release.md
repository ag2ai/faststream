---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 2
hide:
  - navigation
  - footer
---

# Release Notes
## 0.6.4

### What's Changed

* feat: Enables message keys for batch publishing by [@ozeranskii](https://github.com/ozeranskii){.external-link target="_blank"} in [#2586](https://github.com/ag2ai/faststream/pull/2586){.external-link target="_blank"}
* feat: add env variable to disable rich CLI output by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2654](https://github.com/ag2ai/faststream/pull/2654){.external-link target="_blank"}
* feat: Add api for nats message scheduling by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#2640](https://github.com/ag2ai/faststream/pull/2640){.external-link target="_blank"}
* feat: trace is printed only once by [@fil1n](https://github.com/fil1n){.external-link target="_blank"} in [#2661](https://github.com/ag2ai/faststream/pull/2661){.external-link target="_blank"}
* fix: update fast depends config when setting a broker by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2701](https://github.com/ag2ai/faststream/pull/2701){.external-link target="_blank"}
* fix: Dont raise on unknown content types by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2702](https://github.com/ag2ai/faststream/pull/2702){.external-link target="_blank"}
* fix(redis): fix xautoclaim consumption by [@JonathanSerafini](https://github.com/JonathanSerafini){.external-link target="_blank"} in [#2628](https://github.com/ag2ai/faststream/pull/2628){.external-link target="_blank"}
* fix: bug in AsyncAPI schema by [@kittywaresz](https://github.com/kittywaresz){.external-link target="_blank"} in [#2638](https://github.com/ag2ai/faststream/pull/2638){.external-link target="_blank"}
* fix(rabbit): handle leading slash in virtualhost correctly by [@Vitaly312](https://github.com/Vitaly312){.external-link target="_blank"} in [#2707](https://github.com/ag2ai/faststream/pull/2707){.external-link target="_blank"}
* fix: use publisher's exchange when applicable to send reply by [@lachaib](https://github.com/lachaib){.external-link target="_blank"} in [#2655](https://github.com/ag2ai/faststream/pull/2655){.external-link target="_blank"}
* fix: kafka ack policies` behaviour by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2644](https://github.com/ag2ai/faststream/pull/2644){.external-link target="_blank"}
* refactor: streamline conditional logic in `get_one` method of stream … by [@powersemmi](https://github.com/powersemmi){.external-link target="_blank"} in [#2667](https://github.com/ag2ai/faststream/pull/2667){.external-link target="_blank"}
* refactor: restructure justfile and unify cross-platform commands by [@suiseriff](https://github.com/suiseriff){.external-link target="_blank"} in [#2690](https://github.com/ag2ai/faststream/pull/2690){.external-link target="_blank"}
* refactor(docs): actualize kafka how to by [@WorkHardes](https://github.com/WorkHardes){.external-link target="_blank"} in [#2692](https://github.com/ag2ai/faststream/pull/2692){.external-link target="_blank"}
* docs: Added a Code examples article by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2670](https://github.com/ag2ai/faststream/pull/2670){.external-link target="_blank"}
* docs: Fix kafka subscribe docs and llms.txt by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2700](https://github.com/ag2ai/faststream/pull/2700){.external-link target="_blank"}
* docs: Fix link reference in FastAPI integration docs by [@supadrupa](https://github.com/supadrupa){.external-link target="_blank"} in [#2681](https://github.com/ag2ai/faststream/pull/2681){.external-link target="_blank"}
* chore: LICENSE copying added by [@ZoRex15](https://github.com/ZoRex15){.external-link target="_blank"} in [#2685](https://github.com/ag2ai/faststream/pull/2685){.external-link target="_blank"}
* chore: Add warning when using AckPolicy.REJECT_ON_ERROR with kafka by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2686](https://github.com/ag2ai/faststream/pull/2686){.external-link target="_blank"}
* chore: skip relator notifications for dependabot events by @if-i in [#2669](https://github.com/ag2ai/faststream/pull/2669){.external-link target="_blank"}
* chore: added no_confirm=True warning by @roma-frolov in [#2703](https://github.com/ag2ai/faststream/pull/2703){.external-link target="_blank"}
* chore: update relator by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2650](https://github.com/ag2ai/faststream/pull/2650){.external-link target="_blank"}

### New Contributors
* @if-i made their first contribution in [#2669](https://github.com/ag2ai/faststream/pull/2669){.external-link target="_blank"}
* [@supadrupa](https://github.com/supadrupa){.external-link target="_blank"} made their first contribution in [#2681](https://github.com/ag2ai/faststream/pull/2681){.external-link target="_blank"}
* [@ZoRex15](https://github.com/ZoRex15){.external-link target="_blank"} made their first contribution in [#2685](https://github.com/ag2ai/faststream/pull/2685){.external-link target="_blank"}
* [@WorkHardes](https://github.com/WorkHardes){.external-link target="_blank"} made their first contribution in [#2692](https://github.com/ag2ai/faststream/pull/2692){.external-link target="_blank"}
* [@Vitaly312](https://github.com/Vitaly312){.external-link target="_blank"} made their first contribution in [#2707](https://github.com/ag2ai/faststream/pull/2707){.external-link target="_blank"}
* [@lachaib](https://github.com/lachaib){.external-link target="_blank"} made their first contribution in [#2655](https://github.com/ag2ai/faststream/pull/2655){.external-link target="_blank"}

**Full Changelog**: [#0.6.3...0.6.4](https://github.com/ag2ai/faststream/compare/0.6.3...0.6.4){.external-link target="_blank"}

## 0.6.3

### What's Changed

* Fix annotation for group_instance_id parameter by [@gandhis1](https://github.com/gandhis1){.external-link target="_blank"} in [#2606](https://github.com/ag2ai/faststream/pull/2606){.external-link target="_blank"}
* Add support for `min_idle_time` in Redis StreamSub and XAUTOCLAIM by [@powersemmi](https://github.com/powersemmi){.external-link target="_blank"} in [#2607](https://github.com/ag2ai/faststream/pull/2607){.external-link target="_blank"}
* fix incorrect nats annotations by [@swelborn](https://github.com/swelborn){.external-link target="_blank"} in [#2619](https://github.com/ag2ai/faststream/pull/2619){.external-link target="_blank"}
* docs(healthchecks): Fix typing example by [@redb0](https://github.com/redb0){.external-link target="_blank"} in [#2624](https://github.com/ag2ai/faststream/pull/2624){.external-link target="_blank"}
* add nats kv message annotation by [@swelborn](https://github.com/swelborn){.external-link target="_blank"} in [#2626](https://github.com/ag2ai/faststream/pull/2626){.external-link target="_blank"}
* redis: allow tasks to be added to subscribers by [@JonathanSerafini](https://github.com/JonathanSerafini){.external-link target="_blank"} in [#2622](https://github.com/ag2ai/faststream/pull/2622){.external-link target="_blank"}
* Reusing router in multiple brokers or routers by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2591](https://github.com/ag2ai/faststream/pull/2591){.external-link target="_blank"}
* fix: remove UltraJSON (ujson) dependency by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2634](https://github.com/ag2ai/faststream/pull/2634){.external-link target="_blank"}
* fix: allow explicit publishing to partition #0 of a topic from a Kafka publisher by [@antoinehumbert](https://github.com/antoinehumbert){.external-link target="_blank"} in [#2629](https://github.com/ag2ai/faststream/pull/2629){.external-link target="_blank"}
* docs: Versioning policy by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2623](https://github.com/ag2ai/faststream/pull/2623){.external-link target="_blank"}
* docs: Update faststream.md by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2631](https://github.com/ag2ai/faststream/pull/2631){.external-link target="_blank"}
* docs: Added reference to broker's RPC modes by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2636](https://github.com/ag2ai/faststream/pull/2636){.external-link target="_blank"}
* docs: Add Guidilines: Links in documentation by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2637](https://github.com/ag2ai/faststream/pull/2637){.external-link target="_blank"}
* fix enable.auto.commit kafka setting by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2612](https://github.com/ag2ai/faststream/pull/2612){.external-link target="_blank"}
* docs: fix checkboxes by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2643](https://github.com/ag2ai/faststream/pull/2643){.external-link target="_blank"}
* fix: FastAPI 0.121 compat by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2648](https://github.com/ag2ai/faststream/pull/2648){.external-link target="_blank"}

### New Contributors
* [@gandhis1](https://github.com/gandhis1){.external-link target="_blank"} made their first contribution in [#2606](https://github.com/ag2ai/faststream/pull/2606){.external-link target="_blank"}
* [@swelborn](https://github.com/swelborn){.external-link target="_blank"} made their first contribution in [#2619](https://github.com/ag2ai/faststream/pull/2619){.external-link target="_blank"}
* [@redb0](https://github.com/redb0){.external-link target="_blank"} made their first contribution in [#2624](https://github.com/ag2ai/faststream/pull/2624){.external-link target="_blank"}

**Full Changelog**: [#0.6.2...0.6.3](https://github.com/ag2ai/faststream/compare/0.6.2...0.6.3){.external-link target="_blank"}

## 0.6.2

### What's Changed

* Asgi request validation error and docs by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2525](https://github.com/ag2ai/faststream/pull/2525){.external-link target="_blank"}
* fix: docs render if broker is set by set_broker by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2592](https://github.com/ag2ai/faststream/pull/2592){.external-link target="_blank"}
* feat: custom labels in prometheus metrics by @roma-frolov in [#2555](https://github.com/ag2ai/faststream/pull/2555){.external-link target="_blank"}
* Add LICENSE file info in distribution by [@Nifacy](https://github.com/Nifacy){.external-link target="_blank"} in [#2595](https://github.com/ag2ai/faststream/pull/2595){.external-link target="_blank"}
* Mock `xack` and `xdel` methods in Redis testing setup. by [@powersemmi](https://github.com/powersemmi){.external-link target="_blank"} in [#2599](https://github.com/ag2ai/faststream/pull/2599){.external-link target="_blank"}
* fix: correct group_id default usage by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2601](https://github.com/ag2ai/faststream/pull/2601){.external-link target="_blank"}
* fix: force ERROR log level in CriticalLogMiddleware by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2602](https://github.com/ag2ai/faststream/pull/2602){.external-link target="_blank"}
* fix: AsyncAPI 3.0 server security use ref by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2603](https://github.com/ag2ai/faststream/pull/2603){.external-link target="_blank"}
* lint: fix confluent subscriber decorator annotation by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2604](https://github.com/ag2ai/faststream/pull/2604){.external-link target="_blank"}

### New Contributors
* [@Nifacy](https://github.com/Nifacy){.external-link target="_blank"} made their first contribution in [#2595](https://github.com/ag2ai/faststream/pull/2595){.external-link target="_blank"}

**Full Changelog**: [#0.6.1...0.6.2](https://github.com/ag2ai/faststream/compare/0.6.1...0.6.2){.external-link target="_blank"}

## 0.6.1

### What's Changed

* feat: add --loop option to run command by [@dimastbk](https://github.com/dimastbk){.external-link target="_blank"} in [#2572](https://github.com/ag2ai/faststream/pull/2572){.external-link target="_blank"}
* chore(deps): typing extensions to >=4.12.0 by [@maksimv0202](https://github.com/maksimv0202){.external-link target="_blank"} in [#2575](https://github.com/ag2ai/faststream/pull/2575){.external-link target="_blank"}
* chore: update reagento/relator by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2577](https://github.com/ag2ai/faststream/pull/2577){.external-link target="_blank"}
* feat(otel): Allows configurable message counters by [@ozeranskii](https://github.com/ozeranskii){.external-link target="_blank"} in [#2579](https://github.com/ag2ai/faststream/pull/2579){.external-link target="_blank"}
* chore: Updates CODE_OF_CONDUCT link in README.md by [@ozeranskii](https://github.com/ozeranskii){.external-link target="_blank"} in [#2583](https://github.com/ag2ai/faststream/pull/2583){.external-link target="_blank"}
* fix: expose ContextRepo to public API by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2581](https://github.com/ag2ai/faststream/pull/2581){.external-link target="_blank"}
* fix: pass serializer to message in RMQ TestClient by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2582](https://github.com/ag2ai/faststream/pull/2582){.external-link target="_blank"}

### New Contributors
* [@dimastbk](https://github.com/dimastbk){.external-link target="_blank"} made their first contribution in [#2572](https://github.com/ag2ai/faststream/pull/2572){.external-link target="_blank"}
* [@maksimv0202](https://github.com/maksimv0202){.external-link target="_blank"} made their first contribution in [#2575](https://github.com/ag2ai/faststream/pull/2575){.external-link target="_blank"}
* [@ozeranskii](https://github.com/ozeranskii){.external-link target="_blank"} made their first contribution in [#2579](https://github.com/ag2ai/faststream/pull/2579){.external-link target="_blank"}

**Full Changelog**: [#0.6.0...0.6.1](https://github.com/ag2ai/faststream/compare/0.6.0...0.6.1){.external-link target="_blank"}

## 0.6.0

# Description

**FastStream 0.6** is a significant technical release that aimed to address many of the current project design issues and unlock further improvements on the path to version 1.0.0. We tried our best to minimize breaking changes, but unfortunately, some aspects were simply not working well. Therefore, we decided to break them in order to move forward.

This release includes:

* Finalized Middleware API
* Finalized Router API
* Introduced dynamic subscribers
* Added support for various serializer backends (such as [Msgspec](https://github.com/jcrist/msgspec))
* Support for AsyncAPI 3.0 specification
* A range of minor refactors and improvements

The primary goal of this release is to unlock the path towards further features. Therefore, we are pleased to announce that after this release, we plan to work on **MQTT** #956 and **SQS** #794 support and move towards version **1.0.0**!

### Breaking changes

Firstly, we have dropped support for **Python 3.8** and **Python 3.9**. **Python 3.9** is [almost at the end of its life](https://devguide.python.org/versions/) cycle, so it's a good time to update our minimum version.

#### FastStream object changes

The broker has become a POSITIONAL-ONLY argument. This means that `FastStream(broker=broker)` is no longer valid. You should always pass the broker as a separate positional argument, like `FastStream(brokers)`, to ensure proper usage.

This is a preparatory step for `FastStream(*brokers)` support, which will be introduced in **1.0.0**.

#### [AsyncAPI](https://faststream.ag2.ai/latest/getting-started/asyncapi/export/) changes

In **0.6**, you can't directly pass custom AsyncAPI options to the `FastStream` constructor anymore.

```python
app = FastStream(   # doesn't work anymore
    ...,
    title="My App",
    version="1.0.0",
    description="Some description",
)
```

You need to create a `specification` object and pass it manually to the constructor.

```python
from faststream import FastStream, AsyncAPI

FastStream(
    ...
    specification=AsyncAPI(
        title="My App",
        version="1.0.0",
        description="Some description",
    )
)
```

#### Retry feature removed

Previously, you were able to configure retry attempts for a handler by using the following option:

```python
@broker.subscriber("in", retry=True)  # was removed
async def handler(): ...
```

Unfortunately, this option was a design mistake. We apologize for any confusion it may have caused. Technically, it was just a shortcut to `message.nack()` on error. We have decided that manual acknowledgement control would be more idiomatic and better for the framework. Therefore, we have provided a new feature in its place: `ack_policy` control.


```python
@broker.subscriber("test", ack_policy=AckPolicy.ACK_FIRST)
async def handler() -> None: ...
```

With `ack_policy`, you can now control the default acknowledge behavior for your handlers. `AckPolicy` offers the following options:

* **REJECT_ON_ERROR** (default) – to permanently discard messages on failure.
* **NACK_ON_ERROR** – to redeliver messages in case of failure.
* **ACK_FIRST** – for scenarios with high throughput where some message loss can be acceptable.
* **ACK** – if you want the message to be acknowledged, regardless of success or failure.
* **MANUAL** – fully manually control message acknowledgment (for example, calling #!python message.ack() yourself).

In addition, we have deprecated a few more options prior to `ack_policy`.
* `ack_first=True` -> `AckPolicy.ACK_FIRST`
* `no_ack=True` -> `AckPolicy.MANUAL`

#### [Context](https://faststream.ag2.ai/latest/getting-started/context/) changes

We have made some changes to our Dependency Injection system, so the global context is no longer available.

Currently, you cannot simply import the context from anywhere and use it freely.

```python
from faststeam import context  # was removed
```

Instead, you should create the context in a slightly different way. The `FastStream` object serves as an entry point for this, so you can place it wherever you need it:

```python
from typing import Annotated

from faststream import Context, ContextRepo, FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker()

app = FastStream(
    broker,
    context=ContextRepo({
        "global_dependency": "value",
    }),
)
```

Everything else about using the context remains the same. You can request it from the context at any place that supports it.

Additionally, `Context("broker")` and `Context("logger")` have been moved to the local context. They cannot be accessed from lifespan hooks any longer.

```python
@app.after_startup
async def start(
    broker: Broker   # does not work anymore
): ...

@router.subscriber
async def handler(
    broker: Broker   # still working
): ...
```

This change was also made to support multiple brokers.

#### Middlewares changes

Also, we have finalized our Middleware API. It now supports all the features we wanted, and we have no plans to change it anymore. First of all, the `BaseMiddleware` class constructor requires a context (which is no longer global).

```python
class BaseMiddleware:
    def __init__(self, msg: Any | None, context: ContextRepo) -> None:
        self.msg = msg
        self.context = context
```

The context is now available as `self.context` in all middleware methods.

We also changed the `publish_scope` function signature.

```python
class BaseMiddleware:   # old signature
    async def publish_scope(
        self,
        call_next: "AsyncFunc",
        msg: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
```

Previously, any options passed to `brocker.publish("msg", "destination")` had to be consumed as `*args, **kwargs`.

Now, you can consume them all as a single `PublishCommand` object.

```python
from faststream import PublishCommand

class BaseMiddleware:
    async def publish_scope(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any: ...
```

Thanks to **Python 3.13**'s `TypeVars` with defaults, `BaseMiddleware` becomes a generic class and you can specify the `PublishCommand` for the broker you want to work with.

```python
from faststream.rabbit import RabbitPublishCommand

class Middleware(BaseMiddleware[RabbitPublishCommand]):
    async def publish_scope(
        self,
        call_next: Callable[[RabbitPublishCommand], Awaitable[Any]],
        cmd: RabbitPublishCommand,
    ) -> Any: ...
```

Warning: The methods `on_consume`, `after_consume`, `on_publish` and `after_publish` will be deprecated and removed in version **0.7**. Please use `consume_scope` and `publish_scope` instead.

#### Redis Default Message format changes

In **FastStream 0.6** we are using `BinaryMessageFormatV1` as a default instead of `JSONMessageFormat` .
You can find more details in the documentation: https://faststream.ag2.ai/latest/redis/message_format/

### New Features:

1. AsyncAPI3.0 support – now you can choose between `AsyncAPI(schema_version="3.0.0")` (default) and `AsyncAPI(schema_version="2.6.0")` schemas generation

2. [Msgspec](https://github.com/jcrist/msgspec) native support

    ```python
    from fast_depends.msgspec import MsgSpecSerializer

    broker = Broker(serializer=MsgSpecSerializer())
    ```

3. Subscriber iteration support. This features supports all middlewares and other **FastStream** features.

    ```python
    subscriber = broker.subscriber(..., persistent=False)

    await subscriber.start()

    async for msg in subscriber:
        ...
    ```

### Deprecation removed

1. `@broker.subscriber(..., filters=...)` removed
2. `message.decoded_body` removed, use `await message.decode()` instead
3. `publish(..., rpc=True)` removed, use `broker.request()` instead
4. RabbitMQ `@broker.subscriber(..., reply_config=...)` removed, use `Response` instead

### What's Changed
* ConfuentConfig delivery.timeout.ms option added by @stepanbobrik in https://github.com/ag2ai/faststream/pull/2381
* chore: create notification for new issue by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2384
* docs: Add Serialization details & Partial body consuming by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2378
* 0.6.0 by @Lancetnik in https://github.com/ag2ai/faststream/pull/1779
* ci: correct just-install job by @Lancetnik in https://github.com/ag2ai/faststream/pull/2393
* ci: ignore secret detection false positive by @bsoyka in https://github.com/ag2ai/faststream/pull/2399
* fix(redis): assign serializer to internal producer in LogicPublisher by @loRes228 in https://github.com/ag2ai/faststream/pull/2396
* types: add Rabbit type tests  by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2401
* docs: updated manual run with broker examples by @ArtyomVysotskiy in https://github.com/ag2ai/faststream/pull/2402
* tests: remove useless tests by @Lancetnik in https://github.com/ag2ai/faststream/pull/2403
* fix: Added missing DecodedMessage export to faststream.types by @loRes228 in https://github.com/ag2ai/faststream/pull/2405
* types: add Nats type tests by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2406
* types: add Confluent type tests by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2407
* ci: add zizmor and implement related fixes by @bsoyka in https://github.com/ag2ai/faststream/pull/2398
* Docs: fix list of modes by @Totorokrut in https://github.com/ag2ai/faststream/pull/2413
* CI: use pre-commit-ci-lite instead of manual commit step during linters by @kittywaresz in https://github.com/ag2ai/faststream/pull/2416
* lint(rabbit): check publisher and subscriber by @ApostolFet in https://github.com/ag2ai/faststream/pull/2415
* dont replace hyphen in cli values by @borisalekseev in https://github.com/ag2ai/faststream/pull/2426
* docs: update contributing guide with pip upgrade and fix mkdocs serve… by @Kolanar in https://github.com/ag2ai/faststream/pull/2427
* cli: Fix assertion to app object is Application instance by @loRes228 in https://github.com/ag2ai/faststream/pull/2428
* Docs: add llms.txt by @vldmrdev in https://github.com/ag2ai/faststream/pull/2421
* Docs/middlewares main changes by @Maclovi in https://github.com/ag2ai/faststream/pull/2425
* lint: Kafka overrides polish by @Lancetnik in https://github.com/ag2ai/faststream/pull/2429
* Docs: Add example defining custom prometheus metrics to documentation by @Samoed in https://github.com/ag2ai/faststream/pull/2431
* fix: Redis pubsub connection leak in request method by @veronchenko in https://github.com/ag2ai/faststream/pull/2430
* Fix issue 2391 For rabbit and redis fastapi by @ApostolFet in https://github.com/ag2ai/faststream/pull/2437
* Docs: add example with annotated dependencies by @Samoed in https://github.com/ag2ai/faststream/pull/2438
* CI: make the linter great again by @kittywaresz in https://github.com/ag2ai/faststream/pull/2439
* Docs: change scripts folder to `just` by @Samoed in https://github.com/ag2ai/faststream/pull/2436
* Closes #2391 add polish for kafka and nats fastapi by @Flosckow in https://github.com/ag2ai/faststream/pull/2442
* chore: bump version by @Lancetnik in https://github.com/ag2ai/faststream/pull/2440
* fix(asyncapi): promote nested pydantic  to components/schemas by @legau in https://github.com/ag2ai/faststream/pull/2445
* fix: pass stream to concurrent subscribers by @Lancetnik in https://github.com/ag2ai/faststream/pull/2449
* fix(aiokafka): AttributeError on first _LoggingListener.on_partitions_assigned by @legau in https://github.com/ag2ai/faststream/pull/2453
* chore: change issue format by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2465
* docs: Joined context pages into one page by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2460
* fix: add benches.csv, some confluent fixes by @Flosckow in https://github.com/ag2ai/faststream/pull/2461
* docs: Improve cli overview by @borisalekseev in https://github.com/ag2ai/faststream/pull/2414
* Feat: switch image from bitnami to confluent by @Flosckow in https://github.com/ag2ai/faststream/pull/2482
* Feature/update docs by @Kolanar in https://github.com/ag2ai/faststream/pull/2457
* Add new issue workflow by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2481
* fix: 0.6.0rc2 release changes by @Lancetnik in https://github.com/ag2ai/faststream/pull/2485
* 0.6.0rc2 release 08-29-2025 by @Lancetnik in https://github.com/ag2ai/faststream/pull/2454
* ci: change default branch to main back by @Lancetnik in https://github.com/ag2ai/faststream/pull/2488
* ci: create update release PRs to main: by @Lancetnik in https://github.com/ag2ai/faststream/pull/2490
* ci: use PAT to build docs by @Lancetnik in https://github.com/ag2ai/faststream/pull/2491
* ci: update telegram-notifier & send message to oss board by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2492
* ci: explicit user set in docs build pipeline by @Lancetnik in https://github.com/ag2ai/faststream/pull/2494
* fix: incorrect asyncapi render by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2495
* feat: task supervisor by @fil1n in https://github.com/ag2ai/faststream/pull/2408
* docs: Actualize structlog example by @borisalekseev in https://github.com/ag2ai/faststream/pull/2501
* docs: new Gurubase widget token by @Lancetnik in https://github.com/ag2ai/faststream/pull/2502
* chore: update lock by @Lancetnik in https://github.com/ag2ai/faststream/pull/2503
* Fix RuntimeWarning: coroutine 'AsyncMockMixin._execute_mock_call' was… by @maxsonferovante in https://github.com/ag2ai/faststream/pull/2509
* feat: asgi request by @borisalekseev in https://github.com/ag2ai/faststream/pull/2467
* BugFix: add missing serializer in redis test request builder by @JonathanSerafini in https://github.com/ag2ai/faststream/pull/2517
* configure supervisor to use default logging behaviour by @JonathanSerafini in https://github.com/ag2ai/faststream/pull/2518
* feat: add `broker.subscriber(persistent=False)` argument to control WeakRef behavior by @Lancetnik in https://github.com/ag2ai/faststream/pull/2519
* fix: #2513 add magic subscriber name for publisher without routing key by @Lancetnik in https://github.com/ag2ai/faststream/pull/2515
* Improve docs testing section (publisher and subscriber) by @lubaskinc0de in https://github.com/ag2ai/faststream/pull/2521
* docs: fix readme badges by @draincoder in https://github.com/ag2ai/faststream/pull/2523
* docs: Added an example to manual reuse the message's "correlation_id" by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2526
* chore: Remove Doc() part1 by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2532
* ci: use uv ecosystem for dependanbot by @Lancetnik in https://github.com/ag2ai/faststream/pull/2535
* chore: Remove Doc() part2 by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2536
* fix: correct NATS dynamic subscriber stop by @Lancetnik in https://github.com/ag2ai/faststream/pull/2539
* chore: Added Args Doc to StreamSub by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2534
* feature: add warning about idle dynamic subscribers in test clients by @IvanKirpichnikov in https://github.com/ag2ai/faststream/pull/2540
* docs: two phrasing corrections by @axgkl in https://github.com/ag2ai/faststream/pull/2541
* feat: impl ArgsDto to standardize CLI commands by @Lancetnik in https://github.com/ag2ai/faststream/pull/2544
* test: add extended test for 2513 bug by @Lancetnik in https://github.com/ag2ai/faststream/pull/2545
* [ISS-2462] fix(redis): list subscriber consume/stop race by @NelsonNotes in https://github.com/ag2ai/faststream/pull/2531
* cast access_log param to bool by @borisalekseev in https://github.com/ag2ai/faststream/pull/2548
* ci: add GitHub Actions workflow for Telegram notifications on issues and PRs by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2547
* repair doc strings for request method by @HelgeKrueger in https://github.com/ag2ai/faststream/pull/2558
* fix: #2554 respect parser & decoder in all API by @Lancetnik in https://github.com/ag2ai/faststream/pull/2560
* 0.6.0 Release by @Lancetnik in https://github.com/ag2ai/faststream/pull/2569

### New Contributors
* @stepanbobrik made their first contribution in https://github.com/ag2ai/faststream/pull/2381
* @bsoyka made their first contribution in https://github.com/ag2ai/faststream/pull/2399
* @loRes228 made their first contribution in https://github.com/ag2ai/faststream/pull/2396
* @ArtyomVysotskiy made their first contribution in https://github.com/ag2ai/faststream/pull/2402
* @Totorokrut made their first contribution in https://github.com/ag2ai/faststream/pull/2413
* @kittywaresz made their first contribution in https://github.com/ag2ai/faststream/pull/2416
* @Kolanar made their first contribution in https://github.com/ag2ai/faststream/pull/2427
* @vldmrdev made their first contribution in https://github.com/ag2ai/faststream/pull/2421
* @Samoed made their first contribution in https://github.com/ag2ai/faststream/pull/2431
* @veronchenko made their first contribution in https://github.com/ag2ai/faststream/pull/2430
* @legau made their first contribution in https://github.com/ag2ai/faststream/pull/2445
* @fil1n made their first contribution in https://github.com/ag2ai/faststream/pull/2408
* @maxsonferovante made their first contribution in https://github.com/ag2ai/faststream/pull/2509
* @lubaskinc0de made their first contribution in https://github.com/ag2ai/faststream/pull/2521
* @axgkl made their first contribution in https://github.com/ag2ai/faststream/pull/2541
* @HelgeKrueger made their first contribution in https://github.com/ag2ai/faststream/pull/2558

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.48...0.6.0

## v0.6.0

# Description

**FastStream 0.6** is a significant technical release that aimed to address many of the current project design issues and unlock further improvements on the path to version 1.0.0. We tried our best to minimize breaking changes, but unfortunately, some aspects were simply not working well. Therefore, we decided to break them in order to move forward.

This release includes:

* Finalized Middleware API
* Finalized Router API
* Introduced dynamic subscribers
* Added support for various serializer backends (such as [Msgspec](https://github.com/jcrist/msgspec))
* Support for AsyncAPI 3.0 specification
* A range of minor refactors and improvements

The primary goal of this release is to unlock the path towards further features. Therefore, we are pleased to announce that after this release, we plan to work on **MQTT** #956 and **SQS** #794 support and move towards version **1.0.0**!

### Breaking changes

Firstly, we have dropped support for **Python 3.8** and **Python 3.9**. **Python 3.9** is [almost at the end of its life](https://devguide.python.org/versions/) cycle, so it's a good time to update our minimum version.

#### FastStream object changes

The broker has become a POSITIONAL-ONLY argument. This means that `FastStream(broker=broker)` is no longer valid. You should always pass the broker as a separate positional argument, like `FastStream(brokers)`, to ensure proper usage.

This is a preparatory step for `FastStream(*brokers)` support, which will be introduced in **1.0.0**.

#### [AsyncAPI](https://faststream.ag2.ai/latest/getting-started/asyncapi/export/) changes

In **0.6**, you can't directly pass custom AsyncAPI options to the `FastStream` constructor anymore.

```python
app = FastStream(   # doesn't work anymore
    ...,
    title="My App",
    version="1.0.0",
    description="Some description",
)
```

You need to create a `specification` object and pass it manually to the constructor.

```python
from faststream import FastStream, AsyncAPI

FastStream(
    ...
    specification=AsyncAPI(
        title="My App",
        version="1.0.0",
        description="Some description",
    )
)
```

#### Retry feature removed

Previously, you were able to configure retry attempts for a handler by using the following option:

```python
@broker.subscriber("in", retry=True)  # was removed
async def handler(): ...
```

Unfortunately, this option was a design mistake. We apologize for any confusion it may have caused. Technically, it was just a shortcut to `message.nack()` on error. We have decided that manual acknowledgement control would be more idiomatic and better for the framework. Therefore, we have provided a new feature in its place: `ack_policy` control.


```python
@broker.subscriber("test", ack_policy=AckPolicy.ACK_FIRST)
async def handler() -> None: ...
```

With `ack_policy`, you can now control the default acknowledge behavior for your handlers. `AckPolicy` offers the following options:

* **REJECT_ON_ERROR** (default) – to permanently discard messages on failure.
* **NACK_ON_ERROR** – to redeliver messages in case of failure.
* **ACK_FIRST** – for scenarios with high throughput where some message loss can be acceptable.
* **ACK** – if you want the message to be acknowledged, regardless of success or failure.
* **MANUAL** – fully manually control message acknowledgment (for example, calling #!python message.ack() yourself).

In addition, we have deprecated a few more options prior to `ack_policy`.
* `ack_first=True` -> `AckPolicy.ACK_FIRST`
* `no_ack=True` -> `AckPolicy.MANUAL`

#### [Context](https://faststream.ag2.ai/latest/getting-started/context/) changes

We have made some changes to our Dependency Injection system, so the global context is no longer available.

Currently, you cannot simply import the context from anywhere and use it freely.

```python
from faststeam import context  # was removed
```

Instead, you should create the context in a slightly different way. The `FastStream` object serves as an entry point for this, so you can place it wherever you need it:

```python
from typing import Annotated

from faststream import Context, FastStream
from faststream.context import ContextRepo
from faststream.rabbit import RabbitBroker

broker = RabbitBroker()

app = FastStream(
    broker,
    context=ContextRepo({
        "global_dependency": "value",
    }),
)
```

Everything else about using the context remains the same. You can request it from the context at any place that supports it.

Additionally, `Context("broker")` and `Context("logger")` have been moved to the local context. They cannot be accessed from lifespan hooks any longer.

```python
@app.after_startup
async def start(
    broker: Broker   # does not work anymore
): ...

@router.subscriber
async def handler(
    broker: Broker   # still working
): ...
```

This change was also made to support multiple brokers.

#### Middlewares changes

Also, we have finalized our Middleware API. It now supports all the features we wanted, and we have no plans to change it anymore. First of all, the `BaseMiddleware` class constructor requires a context (which is no longer global).

```python
class BaseMiddleware:
    def __init__(self, msg: Any | None, context: ContextRepo) -> None:
        self.msg = msg
        self.context = context
```

The context is now available as `self.context` in all middleware methods.

We also changed the `publish_scope` function signature.

```python
class BaseMiddleware:   # old signature
    async def publish_scope(
        self,
        call_next: "AsyncFunc",
        msg: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
```

Previously, any options passed to `brocker.publish("msg", "destination")` had to be consumed as `*args, **kwargs`.

Now, you can consume them all as a single `PublishCommand` object.

```python
from faststream import PublishCommand

class BaseMiddleware:
    async def publish_scope(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any: ...
```

Thanks to **Python 3.13**'s `TypeVars` with defaults, `BaseMiddleware` becomes a generic class and you can specify the `PublishCommand` for the broker you want to work with.

```python
from faststream.rabbit import RabbitPublishCommand

class Middleware(BaseMiddleware[RabbitPublishCommand]):
    async def publish_scope(
        self,
        call_next: Callable[[RabbitPublishCommand], Awaitable[Any]],
        cmd: RabbitPublishCommand,
    ) -> Any: ...
```

Warning: The methods `on_consume`, `after_consume`, `on_publish` and `after_publish` will be deprecated and removed in version **0.7**. Please use `consume_scope` and `publish_scope` instead.

#### Redis Default Message format changes

In **FastStream 0.6** we are using `BinaryMessageFormatV1` as a default instead of `JSONMessageFormat` .
You can find more details in the documentation: https://faststream.ag2.ai/latest/redis/message_format/

### New Features:

1. AsyncAPI3.0 support – now you can choose between `AsyncAPI(schema_version="3.0.0")` (default) and `AsyncAPI(schema_version="2.6.0")` schemas generation

2. [Msgspec](https://github.com/jcrist/msgspec) native support

    ```python
    from fast_depends.msgspec import MsgSpecSerializer

    broker = Broker(serializer=MsgSpecSerializer())
    ```

3. Subscriber iteration support. This features supports all middlewares and other **FastStream** features.

    ```python
    subscriber = broker.subscriber(..., persistent=False)

    await subscriber.start()

    async for msg in subscriber:
        ...
    ```

### Deprecation removed

1. `@broker.subscriber(..., filters=...)` removed
2. `message.decoded_body` removed, use `await message.decode()` instead
3. `publish(..., rpc=True)` removed, use `broker.request()` instead
4. RabbitMQ `@broker.subscriber(..., reply_config=...)` removed, use `Response` instead

### What's Changed
* ConfuentConfig delivery.timeout.ms option added by @stepanbobrik in https://github.com/ag2ai/faststream/pull/2381
* chore: create notification for new issue by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2384
* docs: Add Serialization details & Partial body consuming by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2378
* 0.6.0 by @Lancetnik in https://github.com/ag2ai/faststream/pull/1779
* ci: correct just-install job by @Lancetnik in https://github.com/ag2ai/faststream/pull/2393
* ci: ignore secret detection false positive by @bsoyka in https://github.com/ag2ai/faststream/pull/2399
* fix(redis): assign serializer to internal producer in LogicPublisher by @loRes228 in https://github.com/ag2ai/faststream/pull/2396
* types: add Rabbit type tests  by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2401
* docs: updated manual run with broker examples by @ArtyomVysotskiy in https://github.com/ag2ai/faststream/pull/2402
* tests: remove useless tests by @Lancetnik in https://github.com/ag2ai/faststream/pull/2403
* fix: Added missing DecodedMessage export to faststream.types by @loRes228 in https://github.com/ag2ai/faststream/pull/2405
* types: add Nats type tests by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2406
* types: add Confluent type tests by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2407
* ci: add zizmor and implement related fixes by @bsoyka in https://github.com/ag2ai/faststream/pull/2398
* Docs: fix list of modes by @Totorokrut in https://github.com/ag2ai/faststream/pull/2413
* CI: use pre-commit-ci-lite instead of manual commit step during linters by @kittywaresz in https://github.com/ag2ai/faststream/pull/2416
* lint(rabbit): check publisher and subscriber by @ApostolFet in https://github.com/ag2ai/faststream/pull/2415
* dont replace hyphen in cli values by @borisalekseev in https://github.com/ag2ai/faststream/pull/2426
* docs: update contributing guide with pip upgrade and fix mkdocs serve… by @Kolanar in https://github.com/ag2ai/faststream/pull/2427
* cli: Fix assertion to app object is Application instance by @loRes228 in https://github.com/ag2ai/faststream/pull/2428
* Docs: add llms.txt by @vldmrdev in https://github.com/ag2ai/faststream/pull/2421
* Docs/middlewares main changes by @Maclovi in https://github.com/ag2ai/faststream/pull/2425
* lint: Kafka overrides polish by @Lancetnik in https://github.com/ag2ai/faststream/pull/2429
* Docs: Add example defining custom prometheus metrics to documentation by @Samoed in https://github.com/ag2ai/faststream/pull/2431
* fix: Redis pubsub connection leak in request method by @veronchenko in https://github.com/ag2ai/faststream/pull/2430
* Fix issue 2391 For rabbit and redis fastapi by @ApostolFet in https://github.com/ag2ai/faststream/pull/2437
* Docs: add example with annotated dependencies by @Samoed in https://github.com/ag2ai/faststream/pull/2438
* CI: make the linter great again by @kittywaresz in https://github.com/ag2ai/faststream/pull/2439
* Docs: change scripts folder to `just` by @Samoed in https://github.com/ag2ai/faststream/pull/2436
* Closes #2391 add polish for kafka and nats fastapi by @Flosckow in https://github.com/ag2ai/faststream/pull/2442
* chore: bump version by @Lancetnik in https://github.com/ag2ai/faststream/pull/2440
* fix(asyncapi): promote nested pydantic  to components/schemas by @legau in https://github.com/ag2ai/faststream/pull/2445
* fix: pass stream to concurrent subscribers by @Lancetnik in https://github.com/ag2ai/faststream/pull/2449
* fix(aiokafka): AttributeError on first _LoggingListener.on_partitions_assigned by @legau in https://github.com/ag2ai/faststream/pull/2453
* chore: change issue format by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2465
* docs: Joined context pages into one page by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2460
* fix: add benches.csv, some confluent fixes by @Flosckow in https://github.com/ag2ai/faststream/pull/2461
* docs: Improve cli overview by @borisalekseev in https://github.com/ag2ai/faststream/pull/2414
* Feat: switch image from bitnami to confluent by @Flosckow in https://github.com/ag2ai/faststream/pull/2482
* Feature/update docs by @Kolanar in https://github.com/ag2ai/faststream/pull/2457
* Add new issue workflow by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2481
* fix: 0.6.0rc2 release changes by @Lancetnik in https://github.com/ag2ai/faststream/pull/2485
* 0.6.0rc2 release 08-29-2025 by @Lancetnik in https://github.com/ag2ai/faststream/pull/2454
* ci: change default branch to main back by @Lancetnik in https://github.com/ag2ai/faststream/pull/2488
* ci: create update release PRs to main: by @Lancetnik in https://github.com/ag2ai/faststream/pull/2490
* ci: use PAT to build docs by @Lancetnik in https://github.com/ag2ai/faststream/pull/2491
* ci: update telegram-notifier & send message to oss board by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2492
* ci: explicit user set in docs build pipeline by @Lancetnik in https://github.com/ag2ai/faststream/pull/2494
* fix: incorrect asyncapi render by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2495
* feat: task supervisor by @fil1n in https://github.com/ag2ai/faststream/pull/2408
* docs: Actualize structlog example by @borisalekseev in https://github.com/ag2ai/faststream/pull/2501
* docs: new Gurubase widget token by @Lancetnik in https://github.com/ag2ai/faststream/pull/2502
* chore: update lock by @Lancetnik in https://github.com/ag2ai/faststream/pull/2503
* Fix RuntimeWarning: coroutine 'AsyncMockMixin._execute_mock_call' was… by @maxsonferovante in https://github.com/ag2ai/faststream/pull/2509
* feat: asgi request by @borisalekseev in https://github.com/ag2ai/faststream/pull/2467
* BugFix: add missing serializer in redis test request builder by @JonathanSerafini in https://github.com/ag2ai/faststream/pull/2517
* configure supervisor to use default logging behaviour by @JonathanSerafini in https://github.com/ag2ai/faststream/pull/2518
* feat: add `broker.subscriber(persistent=False)` argument to control WeakRef behavior by @Lancetnik in https://github.com/ag2ai/faststream/pull/2519
* fix: #2513 add magic subscriber name for publisher without routing key by @Lancetnik in https://github.com/ag2ai/faststream/pull/2515
* Improve docs testing section (publisher and subscriber) by @lubaskinc0de in https://github.com/ag2ai/faststream/pull/2521
* docs: fix readme badges by @draincoder in https://github.com/ag2ai/faststream/pull/2523
* docs: Added an example to manual reuse the message's "correlation_id" by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2526
* chore: Remove Doc() part1 by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2532
* ci: use uv ecosystem for dependanbot by @Lancetnik in https://github.com/ag2ai/faststream/pull/2535
* chore: Remove Doc() part2 by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2536
* fix: correct NATS dynamic subscriber stop by @Lancetnik in https://github.com/ag2ai/faststream/pull/2539
* chore: Added Args Doc to StreamSub by @RenameMe1 in https://github.com/ag2ai/faststream/pull/2534
* feature: add warning about idle dynamic subscribers in test clients by @IvanKirpichnikov in https://github.com/ag2ai/faststream/pull/2540
* docs: two phrasing corrections by @axgkl in https://github.com/ag2ai/faststream/pull/2541
* feat: impl ArgsDto to standardize CLI commands by @Lancetnik in https://github.com/ag2ai/faststream/pull/2544
* test: add extended test for 2513 bug by @Lancetnik in https://github.com/ag2ai/faststream/pull/2545
* [ISS-2462] fix(redis): list subscriber consume/stop race by @NelsonNotes in https://github.com/ag2ai/faststream/pull/2531
* cast access_log param to bool by @borisalekseev in https://github.com/ag2ai/faststream/pull/2548
* ci: add GitHub Actions workflow for Telegram notifications on issues and PRs by @Sehat1137 in https://github.com/ag2ai/faststream/pull/2547
* repair doc strings for request method by @HelgeKrueger in https://github.com/ag2ai/faststream/pull/2558
* fix: #2554 respect parser & decoder in all API by @Lancetnik in https://github.com/ag2ai/faststream/pull/2560
* 0.6.0 Release by @Lancetnik in https://github.com/ag2ai/faststream/pull/2569

### New Contributors
* @stepanbobrik made their first contribution in https://github.com/ag2ai/faststream/pull/2381
* @bsoyka made their first contribution in https://github.com/ag2ai/faststream/pull/2399
* @loRes228 made their first contribution in https://github.com/ag2ai/faststream/pull/2396
* @ArtyomVysotskiy made their first contribution in https://github.com/ag2ai/faststream/pull/2402
* @Totorokrut made their first contribution in https://github.com/ag2ai/faststream/pull/2413
* @kittywaresz made their first contribution in https://github.com/ag2ai/faststream/pull/2416
* @Kolanar made their first contribution in https://github.com/ag2ai/faststream/pull/2427
* @vldmrdev made their first contribution in https://github.com/ag2ai/faststream/pull/2421
* @Samoed made their first contribution in https://github.com/ag2ai/faststream/pull/2431
* @veronchenko made their first contribution in https://github.com/ag2ai/faststream/pull/2430
* @legau made their first contribution in https://github.com/ag2ai/faststream/pull/2445
* @fil1n made their first contribution in https://github.com/ag2ai/faststream/pull/2408
* @maxsonferovante made their first contribution in https://github.com/ag2ai/faststream/pull/2509
* @lubaskinc0de made their first contribution in https://github.com/ag2ai/faststream/pull/2521
* @axgkl made their first contribution in https://github.com/ag2ai/faststream/pull/2541
* @HelgeKrueger made their first contribution in https://github.com/ag2ai/faststream/pull/2558

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.48...0.6.0

## 0.6.0rc4

### What's Changed

This is the latest RC version before the stable release. **0.6.0** is scheduled to be released on 10/10/2025.

* fix: #2462 list subscriber consume/stop race by [@NelsonNotes](https://github.com/NelsonNotes){.external-link target="_blank"} in [#2531](https://github.com/ag2ai/faststream/pull/2531){.external-link target="_blank"}
* fix: correct NATS dynamic subscriber stop by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2539](https://github.com/ag2ai/faststream/pull/2539){.external-link target="_blank"}
* docs: add warning about idle dynamic subscribers in test clients by [@IvanKirpichnikov](https://github.com/IvanKirpichnikov){.external-link target="_blank"} in [#2540](https://github.com/ag2ai/faststream/pull/2540){.external-link target="_blank"}
* docs: fix readme badges by [@draincoder](https://github.com/draincoder){.external-link target="_blank"} in [#2523](https://github.com/ag2ai/faststream/pull/2523){.external-link target="_blank"}
* docs: Added an example to manual reuse the message's "correlation_id" by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2526](https://github.com/ag2ai/faststream/pull/2526){.external-link target="_blank"}
* docs: Added Args Doc to StreamSub by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2534](https://github.com/ag2ai/faststream/pull/2534){.external-link target="_blank"}
* docs: two phrasing corrections by [@axgkl](https://github.com/axgkl){.external-link target="_blank"} in [#2541](https://github.com/ag2ai/faststream/pull/2541){.external-link target="_blank"}
* chore: Remove Doc() part1 by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2532](https://github.com/ag2ai/faststream/pull/2532){.external-link target="_blank"}
* chore: Remove Doc() part2 by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2536](https://github.com/ag2ai/faststream/pull/2536){.external-link target="_blank"}
* refactor: impl ArgsDto to standardize CLI commands by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2544](https://github.com/ag2ai/faststream/pull/2544){.external-link target="_blank"}
* test: add extended test for 2513 bug by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2545](https://github.com/ag2ai/faststream/pull/2545){.external-link target="_blank"}
* ci: use uv ecosystem for dependanbot by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2535](https://github.com/ag2ai/faststream/pull/2535){.external-link target="_blank"}

### New Contributors
* [@axgkl](https://github.com/axgkl){.external-link target="_blank"} made their first contribution in [#2541](https://github.com/ag2ai/faststream/pull/2541){.external-link target="_blank"}

**Full Changelog**: [#0.6.0rc3...0.6.0rc4](https://github.com/ag2ai/faststream/compare/0.6.0rc3...0.6.0rc4){.external-link target="_blank"}

## 0.6.0rc3

### What's Changed
* ci: create update release PRs to main: by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2490](https://github.com/ag2ai/faststream/pull/2490){.external-link target="_blank"}
* ci: use PAT to build docs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2491](https://github.com/ag2ai/faststream/pull/2491){.external-link target="_blank"}
* ci: update telegram-notifier & send message to oss board by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2492](https://github.com/ag2ai/faststream/pull/2492){.external-link target="_blank"}
* ci: explicit user set in docs build pipeline by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2494](https://github.com/ag2ai/faststream/pull/2494){.external-link target="_blank"}
* fix: incorrect asyncapi render by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2495](https://github.com/ag2ai/faststream/pull/2495){.external-link target="_blank"}
* feat: task supervisor by [@fil1n](https://github.com/fil1n){.external-link target="_blank"} in [#2408](https://github.com/ag2ai/faststream/pull/2408){.external-link target="_blank"}
* docs: Actualize structlog example by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2501](https://github.com/ag2ai/faststream/pull/2501){.external-link target="_blank"}
* docs: new Gurubase widget token by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2502](https://github.com/ag2ai/faststream/pull/2502){.external-link target="_blank"}
* chore: update lock by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2503](https://github.com/ag2ai/faststream/pull/2503){.external-link target="_blank"}
* Fix RuntimeWarning: coroutine 'AsyncMockMixin._execute_mock_call' was… by [@maxsonferovante](https://github.com/maxsonferovante){.external-link target="_blank"} in [#2509](https://github.com/ag2ai/faststream/pull/2509){.external-link target="_blank"}
* feat: asgi request by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2467](https://github.com/ag2ai/faststream/pull/2467){.external-link target="_blank"}
* BugFix: add missing serializer in redis test request builder by [@JonathanSerafini](https://github.com/JonathanSerafini){.external-link target="_blank"} in [#2517](https://github.com/ag2ai/faststream/pull/2517){.external-link target="_blank"}
* configure supervisor to use default logging behaviour by [@JonathanSerafini](https://github.com/JonathanSerafini){.external-link target="_blank"} in [#2518](https://github.com/ag2ai/faststream/pull/2518){.external-link target="_blank"}
* feat: add `broker.subscriber(persistent=False)` argument to control WeakRef behavior by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2519](https://github.com/ag2ai/faststream/pull/2519){.external-link target="_blank"}
* fix: #2513 add magic subscriber name for publisher without routing key by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2515](https://github.com/ag2ai/faststream/pull/2515){.external-link target="_blank"}
* Improve docs testing section (publisher and subscriber) by [@lubaskinc0de](https://github.com/lubaskinc0de){.external-link target="_blank"} in [#2521](https://github.com/ag2ai/faststream/pull/2521){.external-link target="_blank"}

### New Contributors
* [@fil1n](https://github.com/fil1n){.external-link target="_blank"} made their first contribution in [#2408](https://github.com/ag2ai/faststream/pull/2408){.external-link target="_blank"}
* [@maxsonferovante](https://github.com/maxsonferovante){.external-link target="_blank"} made their first contribution in [#2509](https://github.com/ag2ai/faststream/pull/2509){.external-link target="_blank"}
* [@lubaskinc0de](https://github.com/lubaskinc0de){.external-link target="_blank"} made their first contribution in [#2521](https://github.com/ag2ai/faststream/pull/2521){.external-link target="_blank"}

**Full Changelog**: [#0.6.0rc2...0.6.0rc3](https://github.com/ag2ai/faststream/compare/0.6.0rc2...0.6.0rc3){.external-link target="_blank"}


## 0.6.0rc2

### What's Changed
* fix(aiokafka): AttributeError on first _LoggingListener.on_partitions_assigned by [@legau](https://github.com/legau){.external-link target="_blank"} in [#2453](https://github.com/ag2ai/faststream/pull/2453){.external-link target="_blank"}
* chore: change issue format by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2465](https://github.com/ag2ai/faststream/pull/2465){.external-link target="_blank"}
* docs: Joined context pages into one page by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2460](https://github.com/ag2ai/faststream/pull/2460){.external-link target="_blank"}
* fix: add benches.csv, some confluent fixes by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#2461](https://github.com/ag2ai/faststream/pull/2461){.external-link target="_blank"}
* docs: Improve cli overview by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2414](https://github.com/ag2ai/faststream/pull/2414){.external-link target="_blank"}
* Feat: switch image from bitnami to confluent by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#2482](https://github.com/ag2ai/faststream/pull/2482){.external-link target="_blank"}
* Feature/update docs by [@Kolanar](https://github.com/Kolanar){.external-link target="_blank"} in [#2457](https://github.com/ag2ai/faststream/pull/2457){.external-link target="_blank"}
* Add new issue workflow by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2481](https://github.com/ag2ai/faststream/pull/2481){.external-link target="_blank"}
* fix: 0.6.0rc2 release changes by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2485](https://github.com/ag2ai/faststream/pull/2485){.external-link target="_blank"}
* 0.6.0rc2 release 08-29-2025 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2454](https://github.com/ag2ai/faststream/pull/2454){.external-link target="_blank"}
* ci: change default branch to main back by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2488](https://github.com/ag2ai/faststream/pull/2488){.external-link target="_blank"}


**Full Changelog**: [#0.6.0rc1...0.6.0rc2](https://github.com/ag2ai/faststream/compare/0.6.0rc1...0.6.0rc2){.external-link target="_blank"}

## 0.6.0rc1

### What's Changed
* ci: correct just-install job by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2393](https://github.com/ag2ai/faststream/pull/2393){.external-link target="_blank"}
* ci: ignore secret detection false positive by [@bsoyka](https://github.com/bsoyka){.external-link target="_blank"} in [#2399](https://github.com/ag2ai/faststream/pull/2399){.external-link target="_blank"}
* fix(redis): assign serializer to internal producer in LogicPublisher by [@loRes228](https://github.com/loRes228){.external-link target="_blank"} in [#2396](https://github.com/ag2ai/faststream/pull/2396){.external-link target="_blank"}
* types: add Rabbit type tests  by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2401](https://github.com/ag2ai/faststream/pull/2401){.external-link target="_blank"}
* docs: updated manual run with broker examples by [@ArtyomVysotskiy](https://github.com/ArtyomVysotskiy){.external-link target="_blank"} in [#2402](https://github.com/ag2ai/faststream/pull/2402){.external-link target="_blank"}
* tests: remove useless tests by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2403](https://github.com/ag2ai/faststream/pull/2403){.external-link target="_blank"}
* fix: Added missing DecodedMessage export to faststream.types by [@loRes228](https://github.com/loRes228){.external-link target="_blank"} in [#2405](https://github.com/ag2ai/faststream/pull/2405){.external-link target="_blank"}
* types: add Nats type tests by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2406](https://github.com/ag2ai/faststream/pull/2406){.external-link target="_blank"}
* types: add Confluent type tests by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2407](https://github.com/ag2ai/faststream/pull/2407){.external-link target="_blank"}
* ci: add zizmor and implement related fixes by [@bsoyka](https://github.com/bsoyka){.external-link target="_blank"} in [#2398](https://github.com/ag2ai/faststream/pull/2398){.external-link target="_blank"}
* Docs: fix list of modes by [@Totorokrut](https://github.com/Totorokrut){.external-link target="_blank"} in [#2413](https://github.com/ag2ai/faststream/pull/2413){.external-link target="_blank"}
* CI: use pre-commit-ci-lite instead of manual commit step during linters by [@kittywaresz](https://github.com/kittywaresz){.external-link target="_blank"} in [#2416](https://github.com/ag2ai/faststream/pull/2416){.external-link target="_blank"}
* lint(rabbit): check publisher and subscriber by [@ApostolFet](https://github.com/ApostolFet){.external-link target="_blank"} in [#2415](https://github.com/ag2ai/faststream/pull/2415){.external-link target="_blank"}
* dont replace hyphen in cli values by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2426](https://github.com/ag2ai/faststream/pull/2426){.external-link target="_blank"}
* docs: update contributing guide with pip upgrade and fix mkdocs serve… by [@Kolanar](https://github.com/Kolanar){.external-link target="_blank"} in [#2427](https://github.com/ag2ai/faststream/pull/2427){.external-link target="_blank"}
* cli: Fix assertion to app object is Application instance by [@loRes228](https://github.com/loRes228){.external-link target="_blank"} in [#2428](https://github.com/ag2ai/faststream/pull/2428){.external-link target="_blank"}
* Docs: add llms.txt by [@vldmrdev](https://github.com/vldmrdev){.external-link target="_blank"} in [#2421](https://github.com/ag2ai/faststream/pull/2421){.external-link target="_blank"}
* Docs/middlewares main changes by [@Maclovi](https://github.com/Maclovi){.external-link target="_blank"} in [#2425](https://github.com/ag2ai/faststream/pull/2425){.external-link target="_blank"}
* lint: Kafka overrides polish by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2429](https://github.com/ag2ai/faststream/pull/2429){.external-link target="_blank"}
* Docs: Add example defining custom prometheus metrics to documentation by [@Samoed](https://github.com/Samoed){.external-link target="_blank"} in [#2431](https://github.com/ag2ai/faststream/pull/2431){.external-link target="_blank"}
* fix: Redis pubsub connection leak in request method by [@veronchenko](https://github.com/veronchenko){.external-link target="_blank"} in [#2430](https://github.com/ag2ai/faststream/pull/2430){.external-link target="_blank"}
* Fix issue 2391 For rabbit and redis fastapi by [@ApostolFet](https://github.com/ApostolFet){.external-link target="_blank"} in [#2437](https://github.com/ag2ai/faststream/pull/2437){.external-link target="_blank"}
* Docs: add example with annotated dependencies by [@Samoed](https://github.com/Samoed){.external-link target="_blank"} in [#2438](https://github.com/ag2ai/faststream/pull/2438){.external-link target="_blank"}
* CI: make the linter great again by [@kittywaresz](https://github.com/kittywaresz){.external-link target="_blank"} in [#2439](https://github.com/ag2ai/faststream/pull/2439){.external-link target="_blank"}
* Docs: change scripts folder to `just` by [@Samoed](https://github.com/Samoed){.external-link target="_blank"} in [#2436](https://github.com/ag2ai/faststream/pull/2436){.external-link target="_blank"}
* Closes #2391 add polish for kafka and nats fastapi by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#2442](https://github.com/ag2ai/faststream/pull/2442){.external-link target="_blank"}
* fix(asyncapi): promote nested pydantic  to components/schemas by [@legau](https://github.com/legau){.external-link target="_blank"} in [#2445](https://github.com/ag2ai/faststream/pull/2445){.external-link target="_blank"}
* fix: pass stream to concurrent subscribers by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2449](https://github.com/ag2ai/faststream/pull/2449){.external-link target="_blank"}

### New Contributors
* [@bsoyka](https://github.com/bsoyka){.external-link target="_blank"} made their first contribution in [#2399](https://github.com/ag2ai/faststream/pull/2399){.external-link target="_blank"}
* [@loRes228](https://github.com/loRes228){.external-link target="_blank"} made their first contribution in [#2396](https://github.com/ag2ai/faststream/pull/2396){.external-link target="_blank"}
* [@ArtyomVysotskiy](https://github.com/ArtyomVysotskiy){.external-link target="_blank"} made their first contribution in [#2402](https://github.com/ag2ai/faststream/pull/2402){.external-link target="_blank"}
* [@Totorokrut](https://github.com/Totorokrut){.external-link target="_blank"} made their first contribution in [#2413](https://github.com/ag2ai/faststream/pull/2413){.external-link target="_blank"}
* [@kittywaresz](https://github.com/kittywaresz){.external-link target="_blank"} made their first contribution in [#2416](https://github.com/ag2ai/faststream/pull/2416){.external-link target="_blank"}
* [@Kolanar](https://github.com/Kolanar){.external-link target="_blank"} made their first contribution in [#2427](https://github.com/ag2ai/faststream/pull/2427){.external-link target="_blank"}
* [@vldmrdev](https://github.com/vldmrdev){.external-link target="_blank"} made their first contribution in [#2421](https://github.com/ag2ai/faststream/pull/2421){.external-link target="_blank"}
* [@Samoed](https://github.com/Samoed){.external-link target="_blank"} made their first contribution in [#2431](https://github.com/ag2ai/faststream/pull/2431){.external-link target="_blank"}
* [@veronchenko](https://github.com/veronchenko){.external-link target="_blank"} made their first contribution in [#2430](https://github.com/ag2ai/faststream/pull/2430){.external-link target="_blank"}
* [@legau](https://github.com/legau){.external-link target="_blank"} made their first contribution in [#2445](https://github.com/ag2ai/faststream/pull/2445){.external-link target="_blank"}

**Full Changelog**: [#0.6.0rc0...0.6.0rc1](https://github.com/ag2ai/faststream/compare/0.6.0rc0...0.6.0rc1){.external-link target="_blank"}

## 0.6.0rc0

# Description

**FastStream 0.6** is a significant technical release that aimed to address many of the current project design issues and unlock further improvements on the path to version 1.0.0. We tried our best to minimize breaking changes, but unfortunately, some aspects were simply not working well. Therefore, we decided to break them in order to move forward.

This release includes:

* Finalized Middleware API
* Finalized Router API
* Introduced dynamic subscribers
* Added support for various serializer backends (such as [Msgspec](https://github.com/jcrist/msgspec))
* Support for AsyncAPI 3.0 specification
* A range of minor refactors and improvements

The primary goal of this release is to unlock the path towards further features. Therefore, we are pleased to announce that after this release, we plan to work on **MQTT** #956 and **SQS** #794 support and move towards version **1.0.0**!

### Breaking changes

Firstly, we have dropped support for **Python 3.8** and **Python 3.9**. **Python 3.9** is [almost at the end of its life](https://devguide.python.org/versions/) cycle, so it's a good time to update our minimum version.

#### FastStream object changes

The broker has become a POSITIONAL-ONLY argument. This means that `FastStream(broker=broker)` is no longer valid. You should always pass the broker as a separate positional argument, like `FastStream(brokers)`, to ensure proper usage.

This is a preparatory step for `FastStream(*brokers)` support, which will be introduced in **1.0.0**.

#### [AsyncAPI](https://faststream.ag2.ai/latest/getting-started/asyncapi/export/) changes

In **0.6**, you can't directly pass custom AsyncAPI options to the `FastStream` constructor anymore.

```python
app = FastStream(   # doesn't work anymore
    ...,
    title="My App",
    version="1.0.0",
    description="Some description",
)
```

You need to create a `specification` object and pass it manually to the constructor.

```python
from faststream import FastStream, AsyncAPI

FastStream(
    ...
    specification=AsyncAPI(
        title="My App",
        version="1.0.0",
        description="Some description",
    )
)
```

#### Retry feature removed

Previously, you were able to configure retry attempts for a handler by using the following option:

```python
@broker.subscriber("in", retry=True)  # was removed
async def handler(): ...
```

Unfortunately, this option was a design mistake. We apologize for any confusion it may have caused. Technically, it was just a shortcut to `message.nack()` on error. We have decided that manual acknowledgement control would be more idiomatic and better for the framework. Therefore, we have provided a new feature in its place: `ack_policy` control.


```python
@broker.subscriber("test", ack_policy=AckPolicy.ACK_FIRST)
async def handler() -> None: ...
```

With `ack_policy`, you can now control the default acknowledge behavior for your handlers. `AckPolicy` offers the following options:

* **REJECT_ON_ERROR** (default) – to permanently discard messages on failure.
* **NACK_ON_ERROR** – to redeliver messages in case of failure.
* **ACK_FIRST** – for scenarios with high throughput where some message loss can be acceptable.
* **ACK** – if you want the message to be acknowledged, regardless of success or failure.
* **MANUAL** – fully manually control message acknowledgment (for example, calling #!python message.ack() yourself).

In addition, we have deprecated a few more options prior to `ack_policy`.
* `ack_first=True` -> `AckPolicy.ACK_FIRST`
* `no_ack=True` -> `AckPolicy.MANUAL`

#### [Context](https://faststream.ag2.ai/latest/getting-started/context/) changes

We have made some changes to our Dependency Injection system, so the global context is no longer available.

Currently, you cannot simply import the context from anywhere and use it freely.

```python
from faststeam import context  # was removed
```

Instead, you should create the context in a slightly different way. The `FastStream` object serves as an entry point for this, so you can place it wherever you need it:

```python
from typing import Annotated

from faststream import Context, FastStream
from faststream.context import ContextRepo
from faststream.rabbit import RabbitBroker

broker = RabbitBroker()

app = FastStream(
    broker,
    context=ContextRepo({
        "global_dependency": "value",
    }),
)
```

Everything else about using the context remains the same. You can request it from the context at any place that supports it.

Additionally, `Context("broker")` and `Context("logger")` have been moved to the local context. They cannot be accessed from lifespan hooks any longer.

```python
@app.after_startup
async def start(
    broker: Broker   # does not work anymore
): ...

@router.subscriber
async def handler(
    broker: Broker   # still working
): ...
```

This change was also made to support multiple brokers.

#### Middlewares changes

Also, we have finalized our Middleware API. It now supports all the features we wanted, and we have no plans to change it anymore. First of all, the `BaseMiddleware` class constructor requires a context (which is no longer global).

```python
class BaseMiddleware:
    def __init__(self, msg: Any | None, context: ContextRepo) -> None:
        self.msg = msg
        self.context = context
```

The context is now available as `self.context` in all middleware methods.

We also changed the `publish_scope` function signature.

```python
class BaseMiddleware:   # old signature
    async def publish_scope(
        self,
        call_next: "AsyncFunc",
        msg: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
```

Previously, any options passed to `brocker.publish("msg", "destination")` had to be consumed as `*args, **kwargs`.

Now, you can consume them all as a single `PublishCommand` object.

```python
from faststream import PublishCommand

class BaseMiddleware:
    async def publish_scope(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any: ...
```

Thanks to **Python 3.13**'s `TypeVars` with defaults, `BaseMiddleware` becomes a generic class and you can specify the `PublishCommand` for the broker you want to work with.

```python
from faststream.rabbit import RabbitPublishCommand

class Middleware(BaseMiddleware[RabbitPublishCommand]):
    async def publish_scope(
        self,
        call_next: Callable[[RabbitPublishCommand], Awaitable[Any]],
        cmd: RabbitPublishCommand,
    ) -> Any: ...
```

Warning: The methods `on_consume`, `after_consume`, `on_publish` and `after_publish` will be deprecated and removed in version **0.7**. Please use `consume_scope` and `publish_scope` instead.

#### Redis Default Message format changes

In **FastStream 0.6** we are using `BinaryMessageFormatV1` as a default instead of `JSONMessageFormat` .
You can find more details in the documentation: https://faststream.ag2.ai/latest/redis/message_format/

### New Features:

1. AsyncAPI3.0 support – now you can choose between `AsyncAPI(schema_version="3.0.0")` (default) and `AsyncAPI(schema_version="2.6.0")` schemas generation

2. [Msgspec](https://github.com/jcrist/msgspec) native support

    ```python
    from fast_depends.msgspec import MsgSpecSerializer

    broker = Broker(serializer=MsgSpecSerializer())
    ```

3. Subscriber iteration support. This features supports all middlewares and other **FastStream** features.

    ```python
    subscriber = broker.subscriber(...)

    await subscriber.start()

    async for msg in subscriber:
        ...
    ```

### Deprecation removed

1. `@broker.subscriber(..., filters=...)` removed
2. `message.decoded_body` removed, use `await message.decode()` instead
3. `publish(..., rpc=True)` removed, use `broker.request()` instead
4. RabbitMQ `@broker.subscriber(..., reply_config=...)` removed, use `Response` instead

### New Contributors
* @stepanbobrik made their first contribution in https://github.com/ag2ai/faststream/pull/2381

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.48...0.6.0rc0

## 0.5.48

### What's Changed

This release is part of the migration to **FastStream 0.6**.

In order to provide great features such as observability and more, **FastStream** requires the inclusion of additional data in your messages. **Redis**, on the other hand, allows for the sending of any type of data within a message. Therefore, with this release, we introduce **FastStream**'s own binary message format, which supports any data type you wish to use and can include additional information.

For more information on the message format, please see the [documentation](https://faststream.ag2.ai/latest/redis/message_format/)

By default, we are still using the JSON message format, but as of version **0.6**, the default will change to the binary format. Therefore, you can prepare your services for this change by manually setting a new protocol.

For whole broker:

```python
from faststream.redis import RedisBroker, BinaryMessageFormatV1

# JSONMessageFormat using by default, but it will be deprecated in future updates
broker = RedisBroker(message_format=BinaryMessageFormatV1)
```

Or for a specifica `subscriber` / `publisher`

```python
from faststream.redis import RedisBroker, BinaryMessageFormatV1

broker = RedisBroker()

@broker.subscriber(..., message_format=BinaryMessageFormatV1)
@broker.publisher(..., message_format=BinaryMessageFormatV1)
async def handler(msg):
    return msg
```

Special thanks for @ilya-4real for this great feature!

**FastStream** require a li
* docs: detach index pages from sections by @Lancetnik in https://github.com/ag2ai/faststream/pull/2358
* chore: Force push gh-pages by @dolfinus in https://github.com/ag2ai/faststream/pull/2360
* fix: #2365 respect prefix by publisher in include_router case by @kodsurfer in https://github.com/ag2ai/faststream/pull/2366
* feature: RMQ reply-to explicit exchange by @vesalius512 in https://github.com/ag2ai/faststream/pull/2368
* Added the ability to specify any start identifier for the xreadgroup by @powersemmi in https://github.com/ag2ai/faststream/pull/2309
* feat: (#2061) Redis Binary Format by @ilya-4real in https://github.com/ag2ai/faststream/pull/2287

### New Contributors
* @kodsurfer made their first contribution in https://github.com/ag2ai/faststream/pull/2366
* @vesalius512 made their first contribution in https://github.com/ag2ai/faststream/pull/2368
* @powersemmi made their first contribution in https://github.com/ag2ai/faststream/pull/2309

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.47...0.5.48

## 0.5.47

### What's Changed
* fix: correct NATS pattern AsyncAPI render by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2354](https://github.com/ag2ai/faststream/pull/2354){.external-link target="_blank"}


**Full Changelog**: [#0.5.46...0.5.47](https://github.com/ag2ai/faststream/compare/0.5.46...0.5.47){.external-link target="_blank"}

## 0.5.46

### What's Changed

* fix overlapping patterns by [@xgemx](https://github.com/xgemx){.external-link target="_blank"} in [#2349](https://github.com/ag2ai/faststream/pull/2349){.external-link target="_blank"}

### New Contributors
* [@xgemx](https://github.com/xgemx){.external-link target="_blank"} made their first contribution in [#2349](https://github.com/ag2ai/faststream/pull/2349){.external-link target="_blank"}

**Full Changelog**: [#0.5.45...0.5.46](https://github.com/ag2ai/faststream/compare/0.5.45...0.5.46){.external-link target="_blank"}

## 0.5.45

### What's Changed
* ci: polish workflows by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2331](https://github.com/ag2ai/faststream/pull/2331){.external-link target="_blank"}
* Update Release Notes for main by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#2337](https://github.com/ag2ai/faststream/pull/2337){.external-link target="_blank"}
* Markdown Code Blocks Auto Set Language by [@mahenzon](https://github.com/mahenzon){.external-link target="_blank"} in [#2339](https://github.com/ag2ai/faststream/pull/2339){.external-link target="_blank"}
* Change docs by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#2343](https://github.com/ag2ai/faststream/pull/2343){.external-link target="_blank"}
* chore(deps): bump the pip group with 5 updates by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#2344](https://github.com/ag2ai/faststream/pull/2344){.external-link target="_blank"}
* ci: fix jobs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2338](https://github.com/ag2ai/faststream/pull/2338){.external-link target="_blank"}

**Full Changelog**: [#0.5.44...0.5.45](https://github.com/ag2ai/faststream/compare/0.5.44...0.5.45){.external-link target="_blank"}

## 0.5.44

### What's Changed

* Cli tests by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2295](https://github.com/ag2ai/faststream/pull/2295){.external-link target="_blank"}
* feature #2091: AsyncAPI HTTP support by [@aligeromachine](https://github.com/aligeromachine){.external-link target="_blank"} in [#2301](https://github.com/ag2ai/faststream/pull/2301){.external-link target="_blank"}
* add try/finally to ensure we exit on close error by [@JonathanSerafini](https://github.com/JonathanSerafini){.external-link target="_blank"} in [#2304](https://github.com/ag2ai/faststream/pull/2304){.external-link target="_blank"}
* docs: fix EmailStr duplication in API Reference by [@AliakseiYafremau](https://github.com/AliakseiYafremau){.external-link target="_blank"} in [#2308](https://github.com/ag2ai/faststream/pull/2308){.external-link target="_blank"}
* Rewrite AsyncAPI docs and log file config tests by [@borisalekseev](https://github.com/borisalekseev){.external-link target="_blank"} in [#2302](https://github.com/ag2ai/faststream/pull/2302){.external-link target="_blank"}
* Add logging kafka errors by [@spataphore1337](https://github.com/spataphore1337){.external-link target="_blank"} in [#2318](https://github.com/ag2ai/faststream/pull/2318){.external-link target="_blank"}
* Add aiokafka error handling by [@spataphore1337](https://github.com/spataphore1337){.external-link target="_blank"} in [#2319](https://github.com/ag2ai/faststream/pull/2319){.external-link target="_blank"}
* feat: add a manual action for deploying from gh-pages branch by [@Tapeline](https://github.com/Tapeline){.external-link target="_blank"} in [#2325](https://github.com/ag2ai/faststream/pull/2325){.external-link target="_blank"}
* Gh pages manual trigger fix by [@Tapeline](https://github.com/Tapeline){.external-link target="_blank"} in [#2326](https://github.com/ag2ai/faststream/pull/2326){.external-link target="_blank"}
* feat: add CODEOWNERS by [@draincoder](https://github.com/draincoder){.external-link target="_blank"} in [#2330](https://github.com/ag2ai/faststream/pull/2330){.external-link target="_blank"}
* introduce stop method on broker and subscriber and deprecate close by [@mahenzon](https://github.com/mahenzon){.external-link target="_blank"} in [#2328](https://github.com/ag2ai/faststream/pull/2328){.external-link target="_blank"}
* fix: add injection extra options in lifespan by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2335](https://github.com/ag2ai/faststream/pull/2335){.external-link target="_blank"}

### New Contributors
* [@aligeromachine](https://github.com/aligeromachine){.external-link target="_blank"} made their first contribution in [#2301](https://github.com/ag2ai/faststream/pull/2301){.external-link target="_blank"}
* [@AliakseiYafremau](https://github.com/AliakseiYafremau){.external-link target="_blank"} made their first contribution in [#2308](https://github.com/ag2ai/faststream/pull/2308){.external-link target="_blank"}
* [@mahenzon](https://github.com/mahenzon){.external-link target="_blank"} made their first contribution in [#2328](https://github.com/ag2ai/faststream/pull/2328){.external-link target="_blank"}

**Full Changelog**: [#0.5.43...0.5.44](https://github.com/ag2ai/faststream/compare/0.5.43...0.5.44){.external-link target="_blank"}

## 0.5.43

### What's Changed

* feat (#2291): add global_qos option to Channel by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2297](https://github.com/ag2ai/faststream/pull/2297){.external-link target="_blank"}
* feat: added delete method to redis message by @ilya-4real in [#2294](https://github.com/ag2ai/faststream/pull/2294){.external-link target="_blank"}
* feat: add CLI extra options by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2231](https://github.com/ag2ai/faststream/pull/2231){.external-link target="_blank"}
* feat: redis pipeline by [@Maclovi](https://github.com/Maclovi){.external-link target="_blank"} in [#2270](https://github.com/ag2ai/faststream/pull/2270){.external-link target="_blank"}
* fix: replace passive with declare by [@Maclovi](https://github.com/Maclovi){.external-link target="_blank"} in [#2236](https://github.com/ag2ai/faststream/pull/2236){.external-link target="_blank"}
* fix: deprecate log fmt by [@Maclovi](https://github.com/Maclovi){.external-link target="_blank"} in [#2240](https://github.com/ag2ai/faststream/pull/2240){.external-link target="_blank"}
* fix (#2252): remove useless code snippet by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2256](https://github.com/ag2ai/faststream/pull/2256){.external-link target="_blank"}
* fix (depends): removed SetupError if correct fastapi.Depends is included to annotation by [@NelsonNotes](https://github.com/NelsonNotes){.external-link target="_blank"} in [#2280](https://github.com/ag2ai/faststream/pull/2280){.external-link target="_blank"}
* Improve license badge by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2241](https://github.com/ag2ai/faststream/pull/2241){.external-link target="_blank"}
* chore: use stricter mypy settings by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2243](https://github.com/ag2ai/faststream/pull/2243){.external-link target="_blank"}
* docs: always use `'` in dependency groupds by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2242](https://github.com/ag2ai/faststream/pull/2242){.external-link target="_blank"}
* docs: better highlight first example by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2245](https://github.com/ag2ai/faststream/pull/2245){.external-link target="_blank"}
* docs: add more links to readme by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2246](https://github.com/ag2ai/faststream/pull/2246){.external-link target="_blank"}
* docs: Improve README code examples by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2247](https://github.com/ag2ai/faststream/pull/2247){.external-link target="_blank"}
* docs: reword `Multiple Subscriptions` section by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2250](https://github.com/ag2ai/faststream/pull/2250){.external-link target="_blank"}
* docs: Improve README code examples by [@mxnoob](https://github.com/mxnoob){.external-link target="_blank"} in [#2253](https://github.com/ag2ai/faststream/pull/2253){.external-link target="_blank"}
* docs: highlight more code by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2255](https://github.com/ag2ai/faststream/pull/2255){.external-link target="_blank"}
* docs: Fix code style in `Context Fields Declaration` by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2269](https://github.com/ag2ai/faststream/pull/2269){.external-link target="_blank"}
* docs: Improve `Context` docs by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2265](https://github.com/ag2ai/faststream/pull/2265){.external-link target="_blank"}
* docs: Reword `publishing/decorator.md` by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2260](https://github.com/ag2ai/faststream/pull/2260){.external-link target="_blank"}
* docs: Improve dependencies docs by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2263](https://github.com/ag2ai/faststream/pull/2263){.external-link target="_blank"}
* docs: Refactor `publishing/index.md` docs by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2259](https://github.com/ag2ai/faststream/pull/2259){.external-link target="_blank"}
* docs: Improve wordings in test docs by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2258](https://github.com/ag2ai/faststream/pull/2258){.external-link target="_blank"}
* docs: Link to test.md from initial tutorial by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2257](https://github.com/ag2ai/faststream/pull/2257){.external-link target="_blank"}
* docs: Improve the documentation page with `Response` class  by [@anywindblows](https://github.com/anywindblows){.external-link target="_blank"} in [#2238](https://github.com/ag2ai/faststream/pull/2238){.external-link target="_blank"}
* docs: fix broken links by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2271](https://github.com/ag2ai/faststream/pull/2271){.external-link target="_blank"}
* docs: Fix `serialization/index.md` wording and syntax by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2277](https://github.com/ag2ai/faststream/pull/2277){.external-link target="_blank"}
* docs: import types from correct place by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2278](https://github.com/ag2ai/faststream/pull/2278){.external-link target="_blank"}
* docs: Improve serialization examples by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2282](https://github.com/ag2ai/faststream/pull/2282){.external-link target="_blank"}
* docs: improve `Lifespan: Hooks` page by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2284](https://github.com/ag2ai/faststream/pull/2284){.external-link target="_blank"}
* docs: -1/+1 fix mistake by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2286](https://github.com/ag2ai/faststream/pull/2286){.external-link target="_blank"}
* docs: doc fix by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2288](https://github.com/ag2ai/faststream/pull/2288){.external-link target="_blank"}
* docs: remove deprecated syntax from examples by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2234](https://github.com/ag2ai/faststream/pull/2234){.external-link target="_blank"}
* docs: Improve grammar in `Lifespan: testing` by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#2298](https://github.com/ag2ai/faststream/pull/2298){.external-link target="_blank"}

### New Contributors
* [@mxnoob](https://github.com/mxnoob){.external-link target="_blank"} made their first contribution in [#2253](https://github.com/ag2ai/faststream/pull/2253){.external-link target="_blank"}
* [@anywindblows](https://github.com/anywindblows){.external-link target="_blank"} made their first contribution in [#2238](https://github.com/ag2ai/faststream/pull/2238){.external-link target="_blank"}
* @ilya-4real made their first contribution in [#2294](https://github.com/ag2ai/faststream/pull/2294){.external-link target="_blank"}

**Full Changelog**: [#0.5.42...0.5.43](https://github.com/ag2ai/faststream/compare/0.5.42...0.5.43){.external-link target="_blank"}

## 0.5.42

### What's Changed

* Feature: add deprecate on retry arg by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#2224](https://github.com/ag2ai/faststream/pull/2224){.external-link target="_blank"}
* Fix default rabbit timestamp by [@dmder](https://github.com/dmder){.external-link target="_blank"} in [#2226](https://github.com/ag2ai/faststream/pull/2226){.external-link target="_blank"}
* Fix ASGIMultiprocess args mismatch by [@dolfinus](https://github.com/dolfinus){.external-link target="_blank"} in [#2228](https://github.com/ag2ai/faststream/pull/2228){.external-link target="_blank"}
* Add docs about cli --log-file by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2229](https://github.com/ag2ai/faststream/pull/2229){.external-link target="_blank"}
* fix (#2227): use ms as generated timestamp in Kafka by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2230](https://github.com/ag2ai/faststream/pull/2230){.external-link target="_blank"}
* feat: Add AsyncAPI HTTP Support by [@tmulligan98](https://github.com/tmulligan98){.external-link target="_blank"} in [#2142](https://github.com/ag2ai/faststream/pull/2142){.external-link target="_blank"}

### New Contributors
* [@dmder](https://github.com/dmder){.external-link target="_blank"} made their first contribution in [#2226](https://github.com/ag2ai/faststream/pull/2226){.external-link target="_blank"}
* [@tmulligan98](https://github.com/tmulligan98){.external-link target="_blank"} made their first contribution in [#2142](https://github.com/ag2ai/faststream/pull/2142){.external-link target="_blank"}

**Full Changelog**: [#0.5.41...0.5.42](https://github.com/ag2ai/faststream/compare/0.5.41...0.5.42){.external-link target="_blank"}

## 0.5.41

### What's Changed

* feature: injection FastAPI in StreamMessage scope by [@IvanKirpichnikov](https://github.com/IvanKirpichnikov){.external-link target="_blank"} in [#2205](https://github.com/ag2ai/faststream/pull/2205){.external-link target="_blank"}
* feat: logging configuration from file by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2203](https://github.com/ag2ai/faststream/pull/2203){.external-link target="_blank"}
* feat: add cluster metadata request via aiokafka admin client in ping by [@murzinov01](https://github.com/murzinov01){.external-link target="_blank"} in [#2212](https://github.com/ag2ai/faststream/pull/2212){.external-link target="_blank"}

### New Contributors
* [@murzinov01](https://github.com/murzinov01){.external-link target="_blank"} made their first contribution in [#2212](https://github.com/ag2ai/faststream/pull/2212){.external-link target="_blank"}

**Full Changelog**: [#0.5.40...0.5.41](https://github.com/ag2ai/faststream/compare/0.5.40...0.5.41){.external-link target="_blank"}

## 0.5.40

### What's Changed
* fix: repair AsyncAPI render by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2175](https://github.com/ag2ai/faststream/pull/2175){.external-link target="_blank"}
* fix: prevent redis stream subscriber timeout when polling_interval exceeds 3000ms by [@caozheliang](https://github.com/caozheliang){.external-link target="_blank"} in [#2200](https://github.com/ag2ai/faststream/pull/2200){.external-link target="_blank"}
* feat: auto flush confluent broker by [@mdaffad](https://github.com/mdaffad){.external-link target="_blank"} in [#2182](https://github.com/ag2ai/faststream/pull/2182){.external-link target="_blank"}
* feat (#2169): Error on faststream.Context instead of faststream.[broker].fastapi.Context by [@NelsonNotes](https://github.com/NelsonNotes){.external-link target="_blank"} in [#2181](https://github.com/ag2ai/faststream/pull/2181){.external-link target="_blank"}
* docs: add information about manual AsyncAPI hosting by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2177](https://github.com/ag2ai/faststream/pull/2177){.external-link target="_blank"}
* docs: update Django integration guide and add FastStream ORM access e… by [@Lodimup](https://github.com/Lodimup){.external-link target="_blank"} in [#2187](https://github.com/ag2ai/faststream/pull/2187){.external-link target="_blank"}
* docs: add  Django management command integration by [@Lodimup](https://github.com/Lodimup){.external-link target="_blank"} in [#2190](https://github.com/ag2ai/faststream/pull/2190){.external-link target="_blank"}
* chore: bump redis to 6.0.0 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2201](https://github.com/ag2ai/faststream/pull/2201){.external-link target="_blank"}
* chore: deprecate connect options by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2202](https://github.com/ag2ai/faststream/pull/2202){.external-link target="_blank"}

### New Contributors
* [@Lodimup](https://github.com/Lodimup){.external-link target="_blank"} made their first contribution in [#2187](https://github.com/ag2ai/faststream/pull/2187){.external-link target="_blank"}
* [@caozheliang](https://github.com/caozheliang){.external-link target="_blank"} made their first contribution in [#2200](https://github.com/ag2ai/faststream/pull/2200){.external-link target="_blank"}

**Full Changelog**: [#0.5.39...0.5.40](https://github.com/ag2ai/faststream/compare/0.5.39...0.5.40){.external-link target="_blank"}

## 0.5.39

### What's Changed
* feat: type client args by [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} in [#2165](https://github.com/ag2ai/faststream/pull/2165){.external-link target="_blank"}
* docs: Added filtering technical information by [@0xWEBMILK](https://github.com/0xWEBMILK){.external-link target="_blank"} in [#2161](https://github.com/ag2ai/faststream/pull/2161){.external-link target="_blank"}
* docs: access the msg attribute of BaseMiddleware by @geth-network in [#2166](https://github.com/ag2ai/faststream/pull/2166){.external-link target="_blank"}
* docs: improve otel docs by [@draincoder](https://github.com/draincoder){.external-link target="_blank"} in [#2167](https://github.com/ag2ai/faststream/pull/2167){.external-link target="_blank"}
* [FS-2158] Explicit error in case of mixed up fastapi.Depends and faststream.Depends by [@NelsonNotes](https://github.com/NelsonNotes){.external-link target="_blank"} in [#2160](https://github.com/ag2ai/faststream/pull/2160){.external-link target="_blank"}
* feat: raise exception at wrong router including by [@bshelkhonov](https://github.com/bshelkhonov){.external-link target="_blank"} in [#2172](https://github.com/ag2ai/faststream/pull/2172){.external-link target="_blank"}
* feat: extend asyncapi view for multiply subscribers by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2174](https://github.com/ag2ai/faststream/pull/2174){.external-link target="_blank"}
* feat: add autoflush on kafka producer by [@mdaffad](https://github.com/mdaffad){.external-link target="_blank"} in [#2170](https://github.com/ag2ai/faststream/pull/2170){.external-link target="_blank"}
* feat: add RabbitMQ Channel object by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2171](https://github.com/ag2ai/faststream/pull/2171){.external-link target="_blank"}

### New Contributors
* @geth-network made their first contribution in [#2166](https://github.com/ag2ai/faststream/pull/2166){.external-link target="_blank"}
* [@mdaffad](https://github.com/mdaffad){.external-link target="_blank"} made their first contribution in [#2170](https://github.com/ag2ai/faststream/pull/2170){.external-link target="_blank"}

**Full Changelog**: [#0.5.38...v0.5.39](https://github.com/ag2ai/faststream/compare/0.5.38...v0.5.39){.external-link target="_blank"}

## 0.5.38

### What's Changed

* Update Release Notes for 0.5.37 by @airt-release-notes-updater in [#2140](https://github.com/ag2ai/faststream/pull/2140){.external-link target="_blank"}
* chore(deps): bump the pip group with 6 updates by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#2144](https://github.com/ag2ai/faststream/pull/2144){.external-link target="_blank"}
* fix: pydantic211 compat by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2150](https://github.com/ag2ai/faststream/pull/2150){.external-link target="_blank"}
* Tweak RabbitMQ queue argument lists by @Arseniy-Popov in [#2148](https://github.com/ag2ai/faststream/pull/2148){.external-link target="_blank"}
* fix: prevent duplicate logging handler registration when calling broker.start() by [@banksemi](https://github.com/banksemi){.external-link target="_blank"} in [#2153](https://github.com/ag2ai/faststream/pull/2153){.external-link target="_blank"}
* chore(deps): bump the pip group with 5 updates by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#2154](https://github.com/ag2ai/faststream/pull/2154){.external-link target="_blank"}
* Move to ag2ai organization by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#2156](https://github.com/ag2ai/faststream/pull/2156){.external-link target="_blank"}

### New Contributors
* [@banksemi](https://github.com/banksemi){.external-link target="_blank"} made their first contribution in [#2153](https://github.com/ag2ai/faststream/pull/2153){.external-link target="_blank"}

**Full Changelog**: [#0.5.37...0.5.38](https://github.com/ag2ai/faststream/compare/0.5.37...0.5.38){.external-link target="_blank"}

## 0.5.37

### What's Changed

* fix: correct Confluent options name mapping by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2137](https://github.com/ag2ai/faststream/pull/2137){.external-link target="_blank"}
* fix: correct call_decorators wrapping order by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2138](https://github.com/ag2ai/faststream/pull/2138){.external-link target="_blank"}

**Full Changelog**: [#0.5.36...0.5.37](https://github.com/ag2ai/faststream/compare/0.5.36...0.5.37){.external-link target="_blank"}

## 0.5.36

### What's Changed

* fix #2088: respect parsed sasl_mechanism by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2092](https://github.com/ag2ai/faststream/pull/2092){.external-link target="_blank"}
* Add delay to unassigned consumer warning by @Arseniy-Popov in [#2090](https://github.com/ag2ai/faststream/pull/2090){.external-link target="_blank"}
* fix: resolve forward refs in FastAPI integration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2096](https://github.com/ag2ai/faststream/pull/2096){.external-link target="_blank"}
* fix: unwrap handler function when getting dependant by @Yakov-Varnaev in [#2093](https://github.com/ag2ai/faststream/pull/2093){.external-link target="_blank"}
* fix: make `CriticalLogMiddleware` respect broker log level (#2130) by [@fadedDexofan](https://github.com/fadedDexofan){.external-link target="_blank"} in [#2131](https://github.com/ag2ai/faststream/pull/2131){.external-link target="_blank"}
* Fixed nested dataclasses schema generation by [@NelsonNotes](https://github.com/NelsonNotes){.external-link target="_blank"} in [#2097](https://github.com/ag2ai/faststream/pull/2097){.external-link target="_blank"}
* refactoring ConfluentConfig by [@diskream](https://github.com/diskream){.external-link target="_blank"} in [#2098](https://github.com/ag2ai/faststream/pull/2098){.external-link target="_blank"}
* refactored subscription Annotation Serialization by [@MaximGit1](https://github.com/MaximGit1){.external-link target="_blank"} in [#2112](https://github.com/ag2ai/faststream/pull/2112){.external-link target="_blank"}
* docs: added pros and cons to publishers page by [@0xWEBMILK](https://github.com/0xWEBMILK){.external-link target="_blank"} in [#2103](https://github.com/ag2ai/faststream/pull/2103){.external-link target="_blank"}
* docs: follow off logging recommendations by [@guitvcer](https://github.com/guitvcer){.external-link target="_blank"} in [#2127](https://github.com/ag2ai/faststream/pull/2127){.external-link target="_blank"}
* docs: add example for probes by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2108](https://github.com/ag2ai/faststream/pull/2108){.external-link target="_blank"}
* docs: fix footer after gurubase integration by [@Rusich90](https://github.com/Rusich90){.external-link target="_blank"} in [#2120](https://github.com/ag2ai/faststream/pull/2120){.external-link target="_blank"}
* docs: Toggle Gurubase widget theme by [@bshelkhonov](https://github.com/bshelkhonov){.external-link target="_blank"} in [#2117](https://github.com/ag2ai/faststream/pull/2117){.external-link target="_blank"}
* docs(#2109): Inlines the include files directly into the location where it is used(faststream.md, scheduling.md) by [@MaestroXXXVIII](https://github.com/MaestroXXXVIII){.external-link target="_blank"} in [#2113](https://github.com/ag2ai/faststream/pull/2113){.external-link target="_blank"}
* docs(#2109): "getting-started/subscription" section has been updated by [@MaximGit1](https://github.com/MaximGit1){.external-link target="_blank"} in [#2114](https://github.com/ag2ai/faststream/pull/2114){.external-link target="_blank"}
* docs(#2109): make PR reference by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2111](https://github.com/ag2ai/faststream/pull/2111){.external-link target="_blank"}
* docs(#2109): inline includes getting-started/lifespan by [@Sabfo](https://github.com/Sabfo){.external-link target="_blank"} in [#2115](https://github.com/ag2ai/faststream/pull/2115){.external-link target="_blank"}
* docs(#2109): inline includes getting-started/dependencies by [@Sabfo](https://github.com/Sabfo){.external-link target="_blank"} in [#2116](https://github.com/ag2ai/faststream/pull/2116){.external-link target="_blank"}
* docs(#2109): inline includes getting-started/serialization/decoder.md by @s-v-y-a-t in [#2118](https://github.com/ag2ai/faststream/pull/2118){.external-link target="_blank"}
* docs(#2109): deleted include code in getting_started/index by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2119](https://github.com/ag2ai/faststream/pull/2119){.external-link target="_blank"}
* docs(#2109): Docs/include inline by @close2code-palm in [#2123](https://github.com/ag2ai/faststream/pull/2123){.external-link target="_blank"}
* docs(#2109): deleted include code in getting_started/opentelemetry by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2125](https://github.com/ag2ai/faststream/pull/2125){.external-link target="_blank"}
* docs(#2109): deleted include code in getting_started/prometheus by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2124](https://github.com/ag2ai/faststream/pull/2124){.external-link target="_blank"}
* docs(#2109): inline includes getting-started/integrations by [@TurtleOld](https://github.com/TurtleOld){.external-link target="_blank"} in [#2121](https://github.com/ag2ai/faststream/pull/2121){.external-link target="_blank"}
* docs(#2109): remove docs includes context, serialization, publishing by [@TurtleOld](https://github.com/TurtleOld){.external-link target="_blank"} in [#2126](https://github.com/ag2ai/faststream/pull/2126){.external-link target="_blank"}

### New Contributors
* [@NelsonNotes](https://github.com/NelsonNotes){.external-link target="_blank"} made their first contribution in [#2097](https://github.com/ag2ai/faststream/pull/2097){.external-link target="_blank"}
* [@diskream](https://github.com/diskream){.external-link target="_blank"} made their first contribution in [#2098](https://github.com/ag2ai/faststream/pull/2098){.external-link target="_blank"}
* [@MaximGit1](https://github.com/MaximGit1){.external-link target="_blank"} made their first contribution in [#2112](https://github.com/ag2ai/faststream/pull/2112){.external-link target="_blank"}
* [@MaestroXXXVIII](https://github.com/MaestroXXXVIII){.external-link target="_blank"} made their first contribution in [#2113](https://github.com/ag2ai/faststream/pull/2113){.external-link target="_blank"}
* [@Sabfo](https://github.com/Sabfo){.external-link target="_blank"} made their first contribution in [#2115](https://github.com/ag2ai/faststream/pull/2115){.external-link target="_blank"}
* @s-v-y-a-t made their first contribution in [#2118](https://github.com/ag2ai/faststream/pull/2118){.external-link target="_blank"}
* [@bshelkhonov](https://github.com/bshelkhonov){.external-link target="_blank"} made their first contribution in [#2117](https://github.com/ag2ai/faststream/pull/2117){.external-link target="_blank"}
* [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} made their first contribution in [#2119](https://github.com/ag2ai/faststream/pull/2119){.external-link target="_blank"}
* @close2code-palm made their first contribution in [#2123](https://github.com/ag2ai/faststream/pull/2123){.external-link target="_blank"}
* [@TurtleOld](https://github.com/TurtleOld){.external-link target="_blank"} made their first contribution in [#2121](https://github.com/ag2ai/faststream/pull/2121){.external-link target="_blank"}
* [@guitvcer](https://github.com/guitvcer){.external-link target="_blank"} made their first contribution in [#2127](https://github.com/ag2ai/faststream/pull/2127){.external-link target="_blank"}
* [@fadedDexofan](https://github.com/fadedDexofan){.external-link target="_blank"} made their first contribution in [#2131](https://github.com/ag2ai/faststream/pull/2131){.external-link target="_blank"}

**Full Changelog**: [#0.5.35...0.5.36](https://github.com/ag2ai/faststream/compare/0.5.35...0.5.36){.external-link target="_blank"}

## 0.5.35

### What's Changed

* Add concurrent-between-partitions kafka subscriber by @Arseniy-Popov in [#2017](https://github.com/ag2ai/faststream/pull/2017){.external-link target="_blank"}
* chore: make uv sync working by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2041](https://github.com/ag2ai/faststream/pull/2041){.external-link target="_blank"}
* docs: add Ask AI button with Gurubase widget by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2042](https://github.com/ag2ai/faststream/pull/2042){.external-link target="_blank"}
* docs: create FastStream image shield by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2062](https://github.com/ag2ai/faststream/pull/2062){.external-link target="_blank"}
* Close #2060 by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#2063](https://github.com/ag2ai/faststream/pull/2063){.external-link target="_blank"}
* feat: allow broker setting in on_startup hook by @Yakov-Varnaev in [#2073](https://github.com/ag2ai/faststream/pull/2073){.external-link target="_blank"}
* ImportError raises change by [@0xWEBMILK](https://github.com/0xWEBMILK){.external-link target="_blank"} in [#2080](https://github.com/ag2ai/faststream/pull/2080){.external-link target="_blank"}
* docs: Fix distributed spelling error in task scheduling page by [@Sandldan](https://github.com/Sandldan){.external-link target="_blank"} in [#2081](https://github.com/ag2ai/faststream/pull/2081){.external-link target="_blank"}
* Exclude confluent-kafka 2.8.1 by @Yakov-Varnaev in [#2084](https://github.com/ag2ai/faststream/pull/2084){.external-link target="_blank"}

### New Contributors
* @Arseniy-Popov made their first contribution in [#2017](https://github.com/ag2ai/faststream/pull/2017){.external-link target="_blank"}
* @Yakov-Varnaev made their first contribution in [#2073](https://github.com/ag2ai/faststream/pull/2073){.external-link target="_blank"}
* [@0xWEBMILK](https://github.com/0xWEBMILK){.external-link target="_blank"} made their first contribution in [#2080](https://github.com/ag2ai/faststream/pull/2080){.external-link target="_blank"}
* [@Sandldan](https://github.com/Sandldan){.external-link target="_blank"} made their first contribution in [#2081](https://github.com/ag2ai/faststream/pull/2081){.external-link target="_blank"}

**Full Changelog**: [#0.5.34...0.5.35](https://github.com/ag2ai/faststream/compare/0.5.34...0.5.35){.external-link target="_blank"}

## 0.5.34

### What's Changed

* fix: when / present in virtual host name and passing as uri by [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} in [#1979](https://github.com/ag2ai/faststream/pull/1979){.external-link target="_blank"}
* fix (#2013): allow to create publisher in already connected broker by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2024](https://github.com/ag2ai/faststream/pull/2024){.external-link target="_blank"}
* feat: add BatchBufferOverflowException by [@spataphore1337](https://github.com/spataphore1337){.external-link target="_blank"} in [#1990](https://github.com/ag2ai/faststream/pull/1990){.external-link target="_blank"}
* feat: add static instrumentation info by [@draincoder](https://github.com/draincoder){.external-link target="_blank"} in [#1996](https://github.com/ag2ai/faststream/pull/1996){.external-link target="_blank"}
* docs: remove reference of "faststream.access" by [@rishabhc32](https://github.com/rishabhc32){.external-link target="_blank"} in [#1995](https://github.com/ag2ai/faststream/pull/1995){.external-link target="_blank"}
* docs: fixed typo in publishing/test.md by [@AlexPetul](https://github.com/AlexPetul){.external-link target="_blank"} in [#2009](https://github.com/ag2ai/faststream/pull/2009){.external-link target="_blank"}
* docs: ability to declare queue/exchange binding by [@MagicAbdel](https://github.com/MagicAbdel){.external-link target="_blank"} in [#2011](https://github.com/ag2ai/faststream/pull/2011){.external-link target="_blank"}
* docs: fix spelling mistake of `/health` by [@herotomg](https://github.com/herotomg){.external-link target="_blank"} in [#2023](https://github.com/ag2ai/faststream/pull/2023){.external-link target="_blank"}
* docs: update aio-pika external docs URL as it has been moved by [@HybridBit](https://github.com/HybridBit){.external-link target="_blank"} in [#1984](https://github.com/ag2ai/faststream/pull/1984){.external-link target="_blank"}
* refactor: add type annotations for RabbitQueue and enum for queue type by [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} in [#2002](https://github.com/ag2ai/faststream/pull/2002){.external-link target="_blank"}

### New Contributors
* [@HybridBit](https://github.com/HybridBit){.external-link target="_blank"} made their first contribution in [#1984](https://github.com/ag2ai/faststream/pull/1984){.external-link target="_blank"}
* [@rishabhc32](https://github.com/rishabhc32){.external-link target="_blank"} made their first contribution in [#1995](https://github.com/ag2ai/faststream/pull/1995){.external-link target="_blank"}
* [@AlexPetul](https://github.com/AlexPetul){.external-link target="_blank"} made their first contribution in [#2009](https://github.com/ag2ai/faststream/pull/2009){.external-link target="_blank"}
* [@MagicAbdel](https://github.com/MagicAbdel){.external-link target="_blank"} made their first contribution in [#2011](https://github.com/ag2ai/faststream/pull/2011){.external-link target="_blank"}
* [@herotomg](https://github.com/herotomg){.external-link target="_blank"} made their first contribution in [#2023](https://github.com/ag2ai/faststream/pull/2023){.external-link target="_blank"}

**Full Changelog**: [#0.5.33...0.5.34](https://github.com/ag2ai/faststream/compare/0.5.33...0.5.34){.external-link target="_blank"}

## 0.5.33

### What's Changed

Just a Confluent & Kafka hotfix. Messages without body (with key only) parsing correctly now.

* fix: Confluent, read messages under lock by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1963](https://github.com/ag2ai/faststream/pull/1963){.external-link target="_blank"}
* fix #1967: correct empty kafka message body processing by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1968](https://github.com/ag2ai/faststream/pull/1968){.external-link target="_blank"}

**Full Changelog**: [#0.5.32...0.5.33](https://github.com/ag2ai/faststream/compare/0.5.32...0.5.33){.external-link target="_blank"}

## 0.5.32

### What's Changed

Thanks to [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} one more time for a new release! Now you have an ability to consume Confluent messages (in autocommit mode) concurrently!

```python
from faststream.confluent import KafkaBroker

broker = KafkaBroker()

@broker.subscriber("topic", max_workers=10)
async def handler():
    """Using `max_workers` option you can process up to 10 messages by one subscriber concurrently"""
```

Also, thanks to [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} for his ASGI CLI support bugfixes

* fix #1959: propagate logger to Confluent by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1960](https://github.com/ag2ai/faststream/pull/1960){.external-link target="_blank"}
* Concurrent confluent kafka by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#1961](https://github.com/ag2ai/faststream/pull/1961){.external-link target="_blank"}
* fix: extend validation for --factory param by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1964](https://github.com/ag2ai/faststream/pull/1964){.external-link target="_blank"}
* fix: support only uvicorn ASGI Runner by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1965](https://github.com/ag2ai/faststream/pull/1965){.external-link target="_blank"}

**Full Changelog**: [#0.5.31...0.5.32](https://github.com/ag2ai/faststream/compare/0.5.31...0.5.32){.external-link target="_blank"}

## 0.5.31

### What's Changed

Well, you (community) made a new breathtaken release for us!
Thanks to all of this release contributors.

Special thanks to [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} . He promotes a new perfect feature - concurrent Kafka subscriber (with autocommit mode)

```python
from faststream.kafka import KafkaBroker

broker = KafkaBroker()

@broker.subscriber("topic", max_workers=10)
async def handler():
    """Using `max_workers` option you can process up to 10 messages by one subscriber concurrently"""
```

Also, thanks to [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} with his ASGI CLI start fixins - now you can use FastStream CLI to scale your AsgiFastStream application by workers

```bash
faststream run main:asgi --workers 2
```

There are a lot of other incredible changes you made:

* feat: add NatsMessage ack_sync method #1906 by [@wpn10](https://github.com/wpn10){.external-link target="_blank"} in [#1909](https://github.com/ag2ai/faststream/pull/1909){.external-link target="_blank"}
* feat: support running ASGI app with Uvicorn using file descriptor by [@minhyeoky](https://github.com/minhyeoky){.external-link target="_blank"} in [#1923](https://github.com/ag2ai/faststream/pull/1923){.external-link target="_blank"}
* feat: Add kafka concurrent subscriber by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#1912](https://github.com/ag2ai/faststream/pull/1912){.external-link target="_blank"}
* fix: bug when using one register for several middleware by @roma-frolov in [#1921](https://github.com/ag2ai/faststream/pull/1921){.external-link target="_blank"}
* fix: change oauth type in asyncapi schema by [@spataphore1337](https://github.com/spataphore1337){.external-link target="_blank"} in [#1926](https://github.com/ag2ai/faststream/pull/1926){.external-link target="_blank"}
* fix: HandlerException ignored by @roma-frolov in [#1928](https://github.com/ag2ai/faststream/pull/1928){.external-link target="_blank"}
* fix: Pomo/nats router by [@Drakorgaur](https://github.com/Drakorgaur){.external-link target="_blank"} in [#1932](https://github.com/ag2ai/faststream/pull/1932){.external-link target="_blank"}
* fix: RabbitBroker's ping is more objective by @roma-frolov in [#1933](https://github.com/ag2ai/faststream/pull/1933){.external-link target="_blank"}
* fix: AsyncAPI 2.6.0 fix empty channels for KafkaSubscriber and ConfluentSubscriber if partitions provided by [@KrySeyt](https://github.com/KrySeyt){.external-link target="_blank"} in [#1930](https://github.com/ag2ai/faststream/pull/1930){.external-link target="_blank"}
* fix: #1874 support workers for ASGI FastStream by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1936](https://github.com/ag2ai/faststream/pull/1936){.external-link target="_blank"}
* fix: correct middlewares order by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#1935](https://github.com/ag2ai/faststream/pull/1935){.external-link target="_blank"}
* chore: run PR altering automated check in same CI job by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1942](https://github.com/ag2ai/faststream/pull/1942){.external-link target="_blank"}
* chore: pin typer version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1947](https://github.com/ag2ai/faststream/pull/1947){.external-link target="_blank"}

### New Contributors

* [@wpn10](https://github.com/wpn10){.external-link target="_blank"} made their first contribution in [#1909](https://github.com/ag2ai/faststream/pull/1909){.external-link target="_blank"}
* [@minhyeoky](https://github.com/minhyeoky){.external-link target="_blank"} made their first contribution in [#1923](https://github.com/ag2ai/faststream/pull/1923){.external-link target="_blank"}

**Full Changelog**: [#0.5.30...0.5.31](https://github.com/ag2ai/faststream/compare/0.5.30...0.5.31){.external-link target="_blank"}

## 0.5.30

### What's Changed
* Introducing FastStream Guru on Gurubase.io by [@kursataktas](https://github.com/kursataktas){.external-link target="_blank"} in [#1903](https://github.com/ag2ai/faststream/pull/1903){.external-link target="_blank"}
* docs: add gurubase badge to the doc by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1905](https://github.com/ag2ai/faststream/pull/1905){.external-link target="_blank"}
* fix: allow users to pass `nkeys_seed_str` as argument for NATS broker. by [@Drakorgaur](https://github.com/Drakorgaur){.external-link target="_blank"} in [#1908](https://github.com/ag2ai/faststream/pull/1908){.external-link target="_blank"}
* Add more warning's to nats subscription factory by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#1907](https://github.com/ag2ai/faststream/pull/1907){.external-link target="_blank"}
* fix: correct working with dependencies versions by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1918](https://github.com/ag2ai/faststream/pull/1918){.external-link target="_blank"}

### New Contributors
* [@kursataktas](https://github.com/kursataktas){.external-link target="_blank"} made their first contribution in [#1903](https://github.com/ag2ai/faststream/pull/1903){.external-link target="_blank"}
* [@Drakorgaur](https://github.com/Drakorgaur){.external-link target="_blank"} made their first contribution in [#1908](https://github.com/ag2ai/faststream/pull/1908){.external-link target="_blank"}

**Full Changelog**: [#0.5.29...0.5.30](https://github.com/ag2ai/faststream/compare/0.5.29...0.5.30){.external-link target="_blank"}

## 0.5.29

### What's Changed

* feat: add explicit message source enum by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1866](https://github.com/ag2ai/faststream/pull/1866){.external-link target="_blank"}
* Change uv manual installation to setup-uv in CI by [@pavelepanov](https://github.com/pavelepanov){.external-link target="_blank"} in [#1871](https://github.com/ag2ai/faststream/pull/1871){.external-link target="_blank"}
* refactor: make Task and Concurrent mixins broker-agnostic by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1873](https://github.com/ag2ai/faststream/pull/1873){.external-link target="_blank"}
* Add support for environment variables in faststream run command by [@ulbwa](https://github.com/ulbwa){.external-link target="_blank"} in [#1876](https://github.com/ag2ai/faststream/pull/1876){.external-link target="_blank"}
* fastapi example update by [@xodiumx](https://github.com/xodiumx){.external-link target="_blank"} in [#1875](https://github.com/ag2ai/faststream/pull/1875){.external-link target="_blank"}
* Do not import `fake_context` if not needed by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#1877](https://github.com/ag2ai/faststream/pull/1877){.external-link target="_blank"}
* build: add warning about manual lifespan_context by [@vectorvp](https://github.com/vectorvp){.external-link target="_blank"} in [#1878](https://github.com/ag2ai/faststream/pull/1878){.external-link target="_blank"}
* Add trending badge by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1882](https://github.com/ag2ai/faststream/pull/1882){.external-link target="_blank"}
* feat: add class method to create a baggage instance from headers by [@vectorvp](https://github.com/vectorvp){.external-link target="_blank"} in [#1885](https://github.com/ag2ai/faststream/pull/1885){.external-link target="_blank"}
* ops: update docker compose commands to compose V2 in scripts by [@vectorvp](https://github.com/vectorvp){.external-link target="_blank"} in [#1889](https://github.com/ag2ai/faststream/pull/1889){.external-link target="_blank"}

### New Contributors
* [@pavelepanov](https://github.com/pavelepanov){.external-link target="_blank"} made their first contribution in [#1871](https://github.com/ag2ai/faststream/pull/1871){.external-link target="_blank"}
* [@xodiumx](https://github.com/xodiumx){.external-link target="_blank"} made their first contribution in [#1875](https://github.com/ag2ai/faststream/pull/1875){.external-link target="_blank"}
* [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} made their first contribution in [#1877](https://github.com/ag2ai/faststream/pull/1877){.external-link target="_blank"}
* [@vectorvp](https://github.com/vectorvp){.external-link target="_blank"} made their first contribution in [#1878](https://github.com/ag2ai/faststream/pull/1878){.external-link target="_blank"}

**Full Changelog**: [#0.5.28...0.5.29](https://github.com/ag2ai/faststream/compare/0.5.28...0.5.29){.external-link target="_blank"}

## 0.5.28

### What's Changed

There were a lot of time since [**0.5.7 OpenTelemetry** release](https://github.com/ag2ai/faststream/releases/tag/0.5.7) and now we completed **Observability** features we planned! **FastStream** supports **Prometheus** metrics in a native way!

Special thanks to @roma-frolov and @draincoder (again) for it!

To collect **Prometheus** metrics for your **FastStream** application you just need to install special distribution

```cmd
pip install 'faststream[prometheus]'
```

And use **PrometheusMiddleware**. Also, it could be helpful to use our [**ASGI**](https://faststream.airt.ai/latest/getting-started/asgi/) to serve metrics endpoint in the same app.

```python
from prometheus_client import CollectorRegistry, make_asgi_app
from faststream.asgi import AsgiFastStream
from faststream.nats import NatsBroker
from faststream.nats.prometheus import NatsPrometheusMiddleware

registry = CollectorRegistry()

broker = NatsBroker(
    middlewares=(
        NatsPrometheusMiddleware(registry=registry),
    )
)

app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry)),
    ]
)
```

Moreover, we have a ready-to-use [**Grafana** dashboard](https://grafana.com/grafana/dashboards/22130-faststream-metrics/) you can just import and use!

To find more information about **Prometheus** support, just visit [our documentation](https://faststream.airt.ai/latest/getting-started/prometheus/).

### All changes

* docs: Correct minimum FastAPI version for lifespan handling by @tim-hutchinson in https://github.com/ag2ai/faststream/pull/1853
* add aiogram example by @IvanKirpichnikov in https://github.com/ag2ai/faststream/pull/1858
* Feature: Prometheus Middleware by @roma-frolov in https://github.com/ag2ai/faststream/pull/1791
* Add in-progress tutorial to how-to section by @sheldygg in https://github.com/ag2ai/faststream/pull/1859
* docs: Add info about Grafana dashboard by @draincoder in https://github.com/ag2ai/faststream/pull/1863

### New Contributors

* @tim-hutchinson made their first contribution in https://github.com/ag2ai/faststream/pull/1853

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.27...0.5.28

## 0.5.27

### What's Changed

* fix: anyio major version parser by [@dotX12](https://github.com/dotX12){.external-link target="_blank"} in [#1850](https://github.com/ag2ai/faststream/pull/1850){.external-link target="_blank"}

### New Contributors
* [@dotX12](https://github.com/dotX12){.external-link target="_blank"} made their first contribution in [#1850](https://github.com/ag2ai/faststream/pull/1850){.external-link target="_blank"}

**Full Changelog**: [#0.5.26...0.5.27](https://github.com/ag2ai/faststream/compare/0.5.26...0.5.27){.external-link target="_blank"}

## 0.5.26

### What's Changed

This it the official **Python 3.13** support! Now, **FastStream** works (and tested) at **Python 3.8 - 3.13** versions!

Warning: **Python3.8** is EOF since **3.13** release and we plan to drop it support in **FastStream 0.6.0** version.

Also, current release has little bugfixes related to **CLI** and **AsyncAPI** schema.

* fix: asgi docs by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1828](https://github.com/ag2ai/faststream/pull/1828){.external-link target="_blank"}
* docs: add link to RU TG community by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1831](https://github.com/ag2ai/faststream/pull/1831){.external-link target="_blank"}
* docs: add dynaconf NATS HowTo example by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#1832](https://github.com/ag2ai/faststream/pull/1832){.external-link target="_blank"}
* Fix AsyncAPI 2.6.0 operation label by [@KrySeyt](https://github.com/KrySeyt){.external-link target="_blank"} in [#1835](https://github.com/ag2ai/faststream/pull/1835){.external-link target="_blank"}
* fix: correct CLI factory behavior by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1838](https://github.com/ag2ai/faststream/pull/1838){.external-link target="_blank"}
* Autocommit precommit changes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1840](https://github.com/ag2ai/faststream/pull/1840){.external-link target="_blank"}
* Add devcontainers supporting all the brokers by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1839](https://github.com/ag2ai/faststream/pull/1839){.external-link target="_blank"}
* Replace async Event with bool by [@Olegt0rr](https://github.com/Olegt0rr){.external-link target="_blank"} in [#1846](https://github.com/ag2ai/faststream/pull/1846){.external-link target="_blank"}
* Add support for Python 3.13 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1845](https://github.com/ag2ai/faststream/pull/1845){.external-link target="_blank"}

**Full Changelog**: [#0.5.25...0.5.26](https://github.com/ag2ai/faststream/compare/0.5.25...0.5.26){.external-link target="_blank"}

## 0.5.25

### What's Changed

* fix: CLI hotfix by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1816](https://github.com/ag2ai/faststream/pull/1816){.external-link target="_blank"}

**Full Changelog**: [#0.5.24...0.5.25](https://github.com/ag2ai/faststream/compare/0.5.24...0.5.25){.external-link target="_blank"}

## 0.5.24

### What's Changed

* Replace while-sleep with Event by [@Olegt0rr](https://github.com/Olegt0rr){.external-link target="_blank"} in [#1683](https://github.com/ag2ai/faststream/pull/1683){.external-link target="_blank"}
* feat: add explicit CLI import error by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1785](https://github.com/ag2ai/faststream/pull/1785){.external-link target="_blank"}
* fix (#1780): replace / in generated json refs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1786](https://github.com/ag2ai/faststream/pull/1786){.external-link target="_blank"}
* Fix: this commit resolve #1765 by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#1789](https://github.com/ag2ai/faststream/pull/1789){.external-link target="_blank"}
* fix (#1792): make RMQ publisher.publish reply_to optional by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1795](https://github.com/ag2ai/faststream/pull/1795){.external-link target="_blank"}
* fix (#1793): FastStream Response support in FastAPI integration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1796](https://github.com/ag2ai/faststream/pull/1796){.external-link target="_blank"}
* Update exception.md by [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} in [#1803](https://github.com/ag2ai/faststream/pull/1803){.external-link target="_blank"}
* Fixes the CI bug that allows PRs with failed tests to be merged. by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1807](https://github.com/ag2ai/faststream/pull/1807){.external-link target="_blank"}
* feat: add CLI support for AsgiFastStream by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1782](https://github.com/ag2ai/faststream/pull/1782){.external-link target="_blank"}
* docs: add contributors page by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1808](https://github.com/ag2ai/faststream/pull/1808){.external-link target="_blank"}
* fix: correct dependency injection of custom context fields implementing partial __eq__/__ne__ by [@antoinehumbert](https://github.com/antoinehumbert){.external-link target="_blank"} in [#1809](https://github.com/ag2ai/faststream/pull/1809){.external-link target="_blank"}
* do not assume discriminator is not a property by @lecko-cngroup in [#1811](https://github.com/ag2ai/faststream/pull/1811){.external-link target="_blank"}

### New Contributors

* [@Olegt0rr](https://github.com/Olegt0rr){.external-link target="_blank"} made their first contribution in [#1683](https://github.com/ag2ai/faststream/pull/1683){.external-link target="_blank"}
* [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} made their first contribution in [#1803](https://github.com/ag2ai/faststream/pull/1803){.external-link target="_blank"}
* [@antoinehumbert](https://github.com/antoinehumbert){.external-link target="_blank"} made their first contribution in [#1809](https://github.com/ag2ai/faststream/pull/1809){.external-link target="_blank"}
* @lecko-cngroup made their first contribution in [#1811](https://github.com/ag2ai/faststream/pull/1811){.external-link target="_blank"}

**Full Changelog**: [#0.5.23...0.5.24](https://github.com/ag2ai/faststream/compare/0.5.23...0.5.24){.external-link target="_blank"}

## 0.5.23

### What's Changed

We made last release just a few days ago, but there are some big changes here already!

1. First of all - you can't use `faststream run ...` command without `pip install 'faststream[cli]'` distribution anymore. It was made to minify default (and production) distribution by removing **typer** (**rich** and **click**) dependencies. **CLI** is a development-time feature, so if you don't need - just don't install! Special thanks to @RubenRibGarcia for this change

2. The next big change - **Kafka** publish confirmations by default! Previous **FastStream** version was working in *publish & forgot* style, but the new one blocks your `broker.publish(...)` call until **Kafka** confirmation frame received. To fallback to previous logic just use a new flag `broker.publish(..., no_confirm=True)`

3. Also, we made one more step forward to our **1.0.0** features plan! @KrySeyt implements `get_one` feature. Now you can use any broker subscriber to get messages in imperative style:

```python
subscriber = broker.subscriber("in")
...
msg = await subscriber.get_one(timeout=5.0)
```

4. And the last one: @draincoder continues to develop OTEL support! Now he provides us with an ability to use **OTEL spans** and **baggage** in a comfortable **FastStream**-style. Just take a look at the [new documentation section](https://faststream.airt.ai/latest/getting-started/opentelemetry/#baggage)

Big thanks to all new and old contributors who makes such a great release!

* feat: AsgiFastStream hooks init options by @Lancetnik in https://github.com/ag2ai/faststream/pull/1768
* fix (#1748): add Kafka publish no_confirm option by @Lancetnik in https://github.com/ag2ai/faststream/pull/1749
* Fix GeneralExceptionHandler typehint by @sheldygg in https://github.com/ag2ai/faststream/pull/1773
* Add `broker.subscriber().get_one()` by @KrySeyt in https://github.com/ag2ai/faststream/pull/1726
* Add OTel baggage support by @draincoder in https://github.com/ag2ai/faststream/pull/1692
* build(#1430): separate cli faststream to its own distribution by @RubenRibGarcia in https://github.com/ag2ai/faststream/pull/1769

### New Contributors
* @RubenRibGarcia made their first contribution in https://github.com/ag2ai/faststream/pull/1769

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.22...0.5.23

## 0.5.22

### What's Changed

* fix: FastAPI 0.112.4+ compatibility by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1766](https://github.com/ag2ai/faststream/pull/1766){.external-link target="_blank"}

**Full Changelog**: [#0.5.21...0.5.22](https://github.com/ag2ai/faststream/compare/0.5.21...0.5.22){.external-link target="_blank"}

## 0.5.21

### What's Changed

* feat (#1168): allow include regular router to FastAPI integration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1747](https://github.com/ag2ai/faststream/pull/1747){.external-link target="_blank"}
* In case if core-subscriber receive a JetStream message. by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#1751](https://github.com/ag2ai/faststream/pull/1751){.external-link target="_blank"}
* feat: explicit final message commit status by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1754](https://github.com/ag2ai/faststream/pull/1754){.external-link target="_blank"}
* Fix/context get local default by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1752](https://github.com/ag2ai/faststream/pull/1752){.external-link target="_blank"}
* fix (#1759): correct ConfluentConfig with enums by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1762](https://github.com/ag2ai/faststream/pull/1762){.external-link target="_blank"}
* Adds SASLOAuthBearer flow to AIO Kafka's Faststream Security Parsing by [@sifex](https://github.com/sifex){.external-link target="_blank"} in [#1761](https://github.com/ag2ai/faststream/pull/1761){.external-link target="_blank"}
* fix: FastAPI 0.112.3 compatibility by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1763](https://github.com/ag2ai/faststream/pull/1763){.external-link target="_blank"}

**Full Changelog**: [#0.5.20...0.5.21](https://github.com/ag2ai/faststream/compare/0.5.20...0.5.21){.external-link target="_blank"}

## 0.5.20

### What's Changed
* Refactor: change publisher fake subscriber generation logic by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1729](https://github.com/ag2ai/faststream/pull/1729){.external-link target="_blank"}
* Remove docs/api directory before running create_api_docs script by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1730](https://github.com/ag2ai/faststream/pull/1730){.external-link target="_blank"}
* CI: automatically approve & merge Dependabot PRs by [@dolfinus](https://github.com/dolfinus){.external-link target="_blank"} in [#1720](https://github.com/ag2ai/faststream/pull/1720){.external-link target="_blank"}
* Run check broken links after docs deploy by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1733](https://github.com/ag2ai/faststream/pull/1733){.external-link target="_blank"}
* Feature: extend FastStream.__init__ by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1734](https://github.com/ag2ai/faststream/pull/1734){.external-link target="_blank"}
* Fix Dependabot group names by [@dolfinus](https://github.com/dolfinus){.external-link target="_blank"} in [#1737](https://github.com/ag2ai/faststream/pull/1737){.external-link target="_blank"}
* Fix: respect ignored exceptions by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1735](https://github.com/ag2ai/faststream/pull/1735){.external-link target="_blank"}
* Fix: update FastAPI to 0.112.2 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1736](https://github.com/ag2ai/faststream/pull/1736){.external-link target="_blank"}


**Full Changelog**: [#0.5.19...0.5.20](https://github.com/ag2ai/faststream/compare/0.5.19...0.5.20){.external-link target="_blank"}

## 0.5.19

### What's Changed

The current release is planned as a latest feature release before **0.6.0**. All other **0.5.19+** releases will contain only minor bugfixes and all the team work will be focused on next major one.

There's a lot of changes we want to present you now though!

#### New RPC feature

Our old `broker.publish(..., rpc=True)` implementation was very limited and ugly. Now we present you a much suitable way to do the same thing - `broker.request(...)`

```python
from faststream import FastStream
from faststream.nats import NatsBroker, NatsResponse, NatsMessage

broker = NatsBroker()

@broker.subscriber("test")
async def echo_handler(msg):
    return NatsResponse(msg, headers={"x-token": "some-token"})

@app.after_startup
async def test():
    # The old implementation was returning just a message body,
    # so you wasn't be able to check response headers, etc
    msg_body: str = await broker.publish("ping", "test", rpc=True)
    assert msg_body == "ping"

    # Now request return the whole message and you can validate any part of it
    # moreover it triggers all your middlewares
    response: NatsMessage = await broker.request("ping", "test")
```

#### Exception Middleware

Community asked and community did! Sorry, we've been putting off this job for too long. Thanks for @Rusich90 to help us!
Now you can wrap your application by a suitable exception handlers. Just check the new [documentation](https://faststream.airt.ai/latest/getting-started/middlewares/exception/) to learn more.

#### Details

Also, there are a lot of minor changes you can find below. Big thanks to all our old and new contributors! You are amazing ones!

* Bug: resolve missing seek on kafka fakeconsumer by @JonathanSerafini in https://github.com/ag2ai/faststream/pull/1682
* replace pip with uv in CI by @newonlynew in https://github.com/ag2ai/faststream/pull/1688
* Added support for JSON serialization and deserialization by other libraries by @ulbwa in https://github.com/ag2ai/faststream/pull/1687
* Fix batch nack by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1689
* Remove unused ignores by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1690
* docs: add Kafka HowTo section by @Lancetnik in https://github.com/ag2ai/faststream/pull/1686
* Add missed out group_instance_id as subscriber and router parameter by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1695
* Set warn_unused_ignores mypy config to true by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1694
* Skip building docs in pre-commit CI job by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1704
* Fix to run check-docs-changes workflow in forks by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1710
* feature/exception_middleware add exception middleware by @Rusich90 in https://github.com/ag2ai/faststream/pull/1604
* Remove mentions of faststream-gen by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1717
* Fix multiple docs issues by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1718
* CI: group Dependabot updates into one PR by @dolfinus in https://github.com/ag2ai/faststream/pull/1719
* feat: CLI DX improvements by @Lancetnik in https://github.com/ag2ai/faststream/pull/1723
* fix: use async test subscribers functions by @Lancetnik in https://github.com/ag2ai/faststream/pull/1725
* feat: add broker.request method by @Lancetnik in https://github.com/ag2ai/faststream/pull/1649

### New Contributors
* @JonathanSerafini made their first contribution in https://github.com/ag2ai/faststream/pull/1682
* @Rusich90 made their first contribution in https://github.com/ag2ai/faststream/pull/1604
* @dolfinus made their first contribution in https://github.com/ag2ai/faststream/pull/1719

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.18...0.5.19

## 0.5.18

### What's Changed

* Added additional parameters to HandlerException by [@ulbwa](https://github.com/ulbwa){.external-link target="_blank"} in [#1659](https://github.com/ag2ai/faststream/pull/1659){.external-link target="_blank"}
* Removed Doc and Added docstrings instead by @Kirill-Stepankov in [#1662](https://github.com/ag2ai/faststream/pull/1662){.external-link target="_blank"}
* feat (#1663): support default values for Header by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1667](https://github.com/ag2ai/faststream/pull/1667){.external-link target="_blank"}
* fix (#1660): correct patch nested JStream subjects by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1666](https://github.com/ag2ai/faststream/pull/1666){.external-link target="_blank"}
* fix: add ConfluentRouter FastAPI missed  init options by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1664](https://github.com/ag2ai/faststream/pull/1664){.external-link target="_blank"}
* Add kerberos support for confluent broker by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1670](https://github.com/ag2ai/faststream/pull/1670){.external-link target="_blank"}
* Fix nack for kafka and confluent brokers by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1678](https://github.com/ag2ai/faststream/pull/1678){.external-link target="_blank"}
* fix: support all RMQ exchanges in AsyncAPI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1679](https://github.com/ag2ai/faststream/pull/1679){.external-link target="_blank"}
* fix: catch parser errors by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1680](https://github.com/ag2ai/faststream/pull/1680){.external-link target="_blank"}

### New Contributors
* [@ulbwa](https://github.com/ulbwa){.external-link target="_blank"} made their first contribution in [#1659](https://github.com/ag2ai/faststream/pull/1659){.external-link target="_blank"}
* @Kirill-Stepankov made their first contribution in [#1662](https://github.com/ag2ai/faststream/pull/1662){.external-link target="_blank"}

**Full Changelog**: [#0.5.17...0.5.18](https://github.com/ag2ai/faststream/compare/0.5.17...0.5.18){.external-link target="_blank"}

## 0.5.17

### What's Changed

Just a hotfix for the following case:

```python
@broker.subscriber(...)
async def handler():
    return NatsResponse(...)

await broker.publish(..., rpc=True)
```

* chore(deps): bump semgrep from 1.83.0 to 1.84.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1650](https://github.com/ag2ai/faststream/pull/1650){.external-link target="_blank"}
* chore(deps): bump mkdocs-material from 9.5.30 to 9.5.31 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1651](https://github.com/ag2ai/faststream/pull/1651){.external-link target="_blank"}
* Update Release Notes for 0.5.16 by @faststream-release-notes-updater in [#1652](https://github.com/ag2ai/faststream/pull/1652){.external-link target="_blank"}
* hotfix: correct NatsResponse processing in RPC case by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1654](https://github.com/ag2ai/faststream/pull/1654){.external-link target="_blank"}


**Full Changelog**: [#0.5.16...0.5.17](https://github.com/ag2ai/faststream/compare/0.5.16...0.5.17){.external-link target="_blank"}

## 0.5.16

### What's Changed

Well, seems like it is the biggest patch release ever 😃

#### Detail Responses

First of all, thanks to all new contributors, who helps us to improve the project! They made a huge impact to this release by adding new Kafka security mechanisms and extend Response API - now you can use `broker.Response` to publish detail information from handler

```python
@broker.subscriber("in")
@broker.publisher("out")
async def handler(msg):
    return Response(msg, headers={"response_header": "Hi!"})   # or KafkaResponse, etc
```

#### ASGI

Also, we added a new huge feature - [**ASGI** support](https://faststream.airt.ai/latest/getting-started/asgi/#other-asgi-compatibility)!

Nope, we are not HTTP-framework now, but it is a little ASGI implementation to provide you with an ability to host documentation, use k8s http-probes and serve metrics in the same with you broker runtime without any dependencies.

You just need to use **AsgiFastStream** class

```python
from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream, make_ping_asgi

from prometheus_client import make_asgi_app
from prometheus_client.registry import CollectorRegistry

broker = NatsBroker()

prometheus_registry = CollectorRegistry()

app = AsgiFastStream(
    broker,
    asyncapi_path="/docs",
    asgi_routes=[
        ("/health", make_ping_asgi(broker, timeout=5.0)),
        ("/metrics", make_asgi_app(registry=prometheus_registry))
    ]
)
```

And then you can run it like a regular ASGI app

```shell
uvicorn main:app
```

#### Confluent partitions

One more thing - manual topic partition assignment for Confluent. We have it already for aiokafka, but missed it here... Now it was fixed!

```python
from faststream.confluent import TopicPartition

@broker.subscriber(partitions=[
    TopicPartition("test-topic", partition=0),
])
async def handler():
    ...
```

#### Detail changes

* feat: add RMQ `fail_fast` option in #1647
* fix: correct nested `NatsRouter` subjects prefixes behavior
* fix typos by @newonlynew in https://github.com/ag2ai/faststream/pull/1609
* Feat: extend response api by @Flosckow in https://github.com/ag2ai/faststream/pull/1607
* Feature: GSSAPI (Kerberos) support by @roma-frolov in https://github.com/ag2ai/faststream/pull/1633
* feat: add oauth support by @filip-danieluk in https://github.com/ag2ai/faststream/pull/1632
* fix: patch broker within testbroker context only by @sfran96 in https://github.com/ag2ai/faststream/pull/1619
* feat: ASGI support by @Lancetnik in https://github.com/ag2ai/faststream/pull/1635

### New Contributors
* @newonlynew made their first contribution in https://github.com/ag2ai/faststream/pull/1609
* @roma-frolov made their first contribution in https://github.com/ag2ai/faststream/pull/1633
* @filip-danieluk made their first contribution in https://github.com/ag2ai/faststream/pull/1632
* @sfran96 made their first contribution in https://github.com/ag2ai/faststream/pull/1619

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.15...0.5.16

## 0.5.15

### What's Changed

Finally, **FastStream** has a Kafka pattern subscription! This is another step forward in our [**Roadmap**](https://github.com/ag2ai/faststream/issues/1510) moving us to **0.6.0** and further!

```python
from faststream import Path
from faststream.kafka import KafkaBroker

broker = KafkaBroker()

@broker.subscriber(pattern="logs.{level}")
async def base_handler(
    body: str,
    level: str = Path(),
):
    ...
```

Also, all brokers now supports a new `ping` method to check real broker connection

```python
is_connected: bool = await broker.ping()
```

This is a little, but important change for [K8S probes](https://github.com/ag2ai/faststream/issues/1181) support

More other there are a lot of bugfixes and improvements from our contributors! Thanks to all of these amazing people!

* feat(multiprocess): restart child processes if they are not alive by @gostilovichd in https://github.com/ag2ai/faststream/pull/1550
* fix: use typing_extensions.TypedDict import by @Lancetnik in https://github.com/ag2ai/faststream/pull/1575
* fix: correct single dataclass argument AsyncAPI payload generation by @Lancetnik in https://github.com/ag2ai/faststream/pull/1591
* fix (#1598): use config with NATS PullSub by @Lancetnik in https://github.com/ag2ai/faststream/pull/1599
* feat: default call_name for broker.subscriber by @KrySeyt in https://github.com/ag2ai/faststream/pull/1589
* Feat: init ping method by @Flosckow in https://github.com/ag2ai/faststream/pull/1592
* chore: bump nats-py requirement by @Lancetnik in https://github.com/ag2ai/faststream/pull/1600
* fix: add pattern checking by @spataphore1337 in https://github.com/ag2ai/faststream/pull/1590

### New Contributors

* @gostilovichd made their first contribution in https://github.com/ag2ai/faststream/pull/1550
* @KrySeyt made their first contribution in https://github.com/ag2ai/faststream/pull/1589
* @Flosckow made their first contribution in https://github.com/ag2ai/faststream/pull/1592

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.14...0.5.15

## 0.5.14

### What's Changed
* Update Release Notes for 0.5.13 by @faststream-release-notes-updater in [#1548](https://github.com/ag2ai/faststream/pull/1548){.external-link target="_blank"}
* Add allow_auto_create_topics to make automatic topic creation configurable by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1556](https://github.com/ag2ai/faststream/pull/1556){.external-link target="_blank"}


**Full Changelog**: [#0.5.13...0.5.14](https://github.com/ag2ai/faststream/compare/0.5.13...0.5.14){.external-link target="_blank"}

## 0.5.13

### What's Changed

* feat: nats filter JS subscription support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1519](https://github.com/ag2ai/faststream/pull/1519){.external-link target="_blank"}
* fix: correct RabbitExchange processing by OTEL in broker.publish case by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1521](https://github.com/ag2ai/faststream/pull/1521){.external-link target="_blank"}
* fix: correct Nats ObjectStorage get file behavior inside watch subscriber by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1523](https://github.com/ag2ai/faststream/pull/1523){.external-link target="_blank"}
* Resolve Issue 1386, Add rpc_prefix by [@aKardasz](https://github.com/aKardasz){.external-link target="_blank"} in [#1484](https://github.com/ag2ai/faststream/pull/1484){.external-link target="_blank"}
* fix: correct spans linking in batches case by [@draincoder](https://github.com/draincoder){.external-link target="_blank"} in [#1532](https://github.com/ag2ai/faststream/pull/1532){.external-link target="_blank"}
* fix (#1539): correct anyio.create_memory_object_stream annotation by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1541](https://github.com/ag2ai/faststream/pull/1541){.external-link target="_blank"}
* fix: correct publish_coverage CI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1536](https://github.com/ag2ai/faststream/pull/1536){.external-link target="_blank"}
* Add NatsBroker.new_inbox() by [@maxalbert](https://github.com/maxalbert){.external-link target="_blank"} in [#1543](https://github.com/ag2ai/faststream/pull/1543){.external-link target="_blank"}
* fix (#1544): correct Redis message nack & reject signature by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1546](https://github.com/ag2ai/faststream/pull/1546){.external-link target="_blank"}

### New Contributors
* [@aKardasz](https://github.com/aKardasz){.external-link target="_blank"} made their first contribution in [#1484](https://github.com/ag2ai/faststream/pull/1484){.external-link target="_blank"}
* [@maxalbert](https://github.com/maxalbert){.external-link target="_blank"} made their first contribution in [#1543](https://github.com/ag2ai/faststream/pull/1543){.external-link target="_blank"}

**Full Changelog**: [#0.5.12...0.5.13](https://github.com/ag2ai/faststream/compare/0.5.12...0.5.13){.external-link target="_blank"}

## 0.5.12

### What's Changed

Now, `FastStream` provides users with the ability to pass the `config` dictionary to `confluent-kafka-python` for greater customizability. The following example sets the parameter `topic.metadata.refresh.fast.interval.ms`'s value to `300` instead of the default value `100` via the `config` parameter.

```python
from faststream import FastStream
from faststream.confluent import KafkaBroker

config = {"topic.metadata.refresh.fast.interval.ms": 300}
broker = KafkaBroker("localhost:9092", config=config)
app = FastStream(broker)
```

* Update Release Notes for 0.5.11 by @faststream-release-notes-updater in [#1511](https://github.com/ag2ai/faststream/pull/1511){.external-link target="_blank"}
* docs: update filters example by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1516](https://github.com/ag2ai/faststream/pull/1516){.external-link target="_blank"}
* Add config param to pass additional parameters to confluent-kafka-python by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1505](https://github.com/ag2ai/faststream/pull/1505){.external-link target="_blank"}


**Full Changelog**: [#0.5.11...0.5.12](https://github.com/ag2ai/faststream/compare/0.5.11...0.5.12){.external-link target="_blank"}

## 0.5.11

### What's Changed
* Update Release Notes for 0.5.10 by @faststream-release-notes-updater in [#1482](https://github.com/ag2ai/faststream/pull/1482){.external-link target="_blank"}
* feat: provide with an ability to create default RMQ Exchange by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1485](https://github.com/ag2ai/faststream/pull/1485){.external-link target="_blank"}
* docs: fix typos by [@crazymidnight](https://github.com/crazymidnight){.external-link target="_blank"} in [#1489](https://github.com/ag2ai/faststream/pull/1489){.external-link target="_blank"}
* chore: update CI triggers to minify useless runs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1483](https://github.com/ag2ai/faststream/pull/1483){.external-link target="_blank"}
* Update link to badges by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1496](https://github.com/ag2ai/faststream/pull/1496){.external-link target="_blank"}
* Run tests every day at 12:00 AM by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1497](https://github.com/ag2ai/faststream/pull/1497){.external-link target="_blank"}
* Chore: update deps by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1503](https://github.com/ag2ai/faststream/pull/1503){.external-link target="_blank"}
* fix: include NatsRouter streams to original broker by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1509](https://github.com/ag2ai/faststream/pull/1509){.external-link target="_blank"}

### New Contributors
* [@crazymidnight](https://github.com/crazymidnight){.external-link target="_blank"} made their first contribution in [#1489](https://github.com/ag2ai/faststream/pull/1489){.external-link target="_blank"}

**Full Changelog**: [#0.5.10...0.5.11](https://github.com/ag2ai/faststream/compare/0.5.10...0.5.11){.external-link target="_blank"}

## 0.5.10

### What's Changed

Now you can return Response class to set more specific outgoing message parameters:

```python
from faststream import Response

@broker.subscriber("in")
@broker.subscriber("out")
async def handler():
    return Response(body=b"", headers={})
```

* Pass logger to confluent producer and consumer by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1464](https://github.com/ag2ai/faststream/pull/1464){.external-link target="_blank"}
* Fixes  #1412 with `TestKafkaBroker` behaviour where Consumer Groups weren't being respected by [@sifex](https://github.com/sifex){.external-link target="_blank"} in [#1413](https://github.com/ag2ai/faststream/pull/1413){.external-link target="_blank"}
* Chore: update dependency versions by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1478](https://github.com/ag2ai/faststream/pull/1478){.external-link target="_blank"}
* Remove typing-extensions version restriction by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1477](https://github.com/ag2ai/faststream/pull/1477){.external-link target="_blank"}
* feat (#1431): add Response class by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1481](https://github.com/ag2ai/faststream/pull/1481){.external-link target="_blank"}

### New Contributors
* [@sifex](https://github.com/sifex){.external-link target="_blank"} made their first contribution in [#1413](https://github.com/ag2ai/faststream/pull/1413){.external-link target="_blank"}

**Full Changelog**: [#0.5.9...0.5.10](https://github.com/ag2ai/faststream/compare/0.5.9...0.5.10){.external-link target="_blank"}

## 0.5.9

### What's Changed
* Update Release Notes for 0.5.8 by @faststream-release-notes-updater in [#1462](https://github.com/ag2ai/faststream/pull/1462){.external-link target="_blank"}
* Exclude typing_extensions version 4.12.* by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1467](https://github.com/ag2ai/faststream/pull/1467){.external-link target="_blank"}
* fix: add group/consumer to hash to avoid overwriting by [@fbraem](https://github.com/fbraem){.external-link target="_blank"} in [#1463](https://github.com/ag2ai/faststream/pull/1463){.external-link target="_blank"}
* Bump version to 0.5.9 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1468](https://github.com/ag2ai/faststream/pull/1468){.external-link target="_blank"}

### New Contributors
* [@fbraem](https://github.com/fbraem){.external-link target="_blank"} made their first contribution in [#1463](https://github.com/ag2ai/faststream/pull/1463){.external-link target="_blank"}

**Full Changelog**: [#0.5.8...0.5.9](https://github.com/ag2ai/faststream/compare/0.5.8...0.5.9){.external-link target="_blank"}

## 0.5.8

### What's Changed

This is the time for a new **NATS** features! **FastStream** supports **NATS Key-Value** and **Object Storage** subscription features in a native way now (big thx for @sheldygg)!

1. KeyValue creation and watching API added (you can read updated [documentation section](https://faststream.airt.ai/latest/nats/jetstream/key-value/) for changes):

   ```python
    from faststream import FastStream, Logger
    from faststream.nats import NatsBroker

    broker = NatsBroker()
    app = FastStream(broker)

    @broker.subscriber("some-key", kv_watch="bucket")
    async def handler(msg: int, logger: Logger):
        logger.info(msg)

    @app.after_startup
    async def test():
        kv = await broker.key_value("bucket")
        await kv.put("some-key", b"1")
   ```

2. ObjectStore API added as well (you can read updated [documentation section](https://faststream.airt.ai/latest/nats/jetstream/object/) for changes):

    ```python
    from faststream import FastStream, Logger
    from faststream.nats import NatsBroker

    broker = NatsBroker()
    app = FastStream(broker)

    @broker.subscriber("file-bucket", obj_watch=True)
    async def handler(filename: str, logger: Logger):
        logger.info(filename)

    @app.after_startup
    async def test():
        object_store = await broker.object_storage("file-bucket")
        await object_store.put("some-file.txt", b"1")
    ```

3. Also now you can use just `pull_sub=True` instead of `pull_sub=PullSub()` in basic case:

   ```python
    from faststream import FastStream, Logger
    from faststream.nats import NatsBroker

    broker = NatsBroker()
    app = FastStream(broker)

    @broker.subscriber("test", stream="stream", pull_sub=True)
    async def handler(msg, logger: Logger):
        logger.info(msg)
    ```

Finally, we have a new feature, related to all brokers: special flag to suppress automatic RPC and reply_to responses:

```python
@broker.subscriber("tests", no_reply=True)
async def handler():
    ....

# will fail with timeout, because there is no automatic response
msg = await broker.publish("msg", "test", rpc=True)
```

* fix: when headers() returns None in AsyncConfluentParser, replace it with an empty tuple by @andreaimprovised in https://github.com/ag2ai/faststream/pull/1460
* Implement Kv/Obj watch. by @sheldygg in https://github.com/ag2ai/faststream/pull/1383
* feat: add subscriber no-reply option by @Lancetnik in https://github.com/ag2ai/faststream/pull/1461

### New Contributors
* @andreaimprovised made their first contribution in https://github.com/ag2ai/faststream/pull/1460

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.7...0.5.8

## 0.5.7

### What's Changed

Finally, FastStream supports [OpenTelemetry](https://opentelemetry.io/) in a native way to collect the full trace of your services! Big thanks for @draincoder for that!

First of all you need to install required dependencies to support OpenTelemetry:

```bash
pip install 'faststream[otel]'
```

Then you can just add a middleware for your broker and that's it!

```python
from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.nats.opentelemetry import NatsTelemetryMiddleware

broker = NatsBroker(
    middlewares=(
        NatsTelemetryMiddleware(),
    )
)
app = FastStream(broker)
```

To find detail information just visit our documentation about [telemetry](https://faststream.airt.ai/latest/getting-started/opentelemetry/)

P.S. The release includes basic OpenTelemetry support - messages tracing & basic metrics. Baggage support and correct spans linking in batch processing case will be added soon.

* fix: serialize TestClient rpc output to mock the real message by @Lancetnik in https://github.com/ag2ai/faststream/pull/1452
* feature (#916): Observability by @draincoder in https://github.com/ag2ai/faststream/pull/1398

### New Contributors
* @draincoder made their first contribution in https://github.com/ag2ai/faststream/pull/1398

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.6...0.5.7

## 0.5.6

### What's Changed

* feature: add --factory param by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1440](https://github.com/ag2ai/faststream/pull/1440){.external-link target="_blank"}
* feat: add RMQ channels options, support for prefix for routing_key, a… by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1448](https://github.com/ag2ai/faststream/pull/1448){.external-link target="_blank"}
* feature: Add `from faststream.rabbit.annotations import Connection, Channel` shortcuts
* Bugfix: RabbitMQ RabbitRouter prefix now affects to queue routing key as well
* Feature (close #1402): add `broker.add_middleware` public API to append a middleware to already created broker
* Feature: add `RabbitBroker(channel_number: int, publisher_confirms: bool, on_return_raises: bool)` options to setup channel settings
* Feature (close #1447): add `StreamMessage.batch_headers` attribute to provide with access to whole batch messages headers

### New Contributors

* [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} made their first contribution in [#1440](https://github.com/ag2ai/faststream/pull/1440){.external-link target="_blank"}

**Full Changelog**: [#0.5.5...0.5.6](https://github.com/ag2ai/faststream/compare/0.5.5...0.5.6){.external-link target="_blank"}

## 0.5.5

### What's Changed

Add support for explicit partition assignment in aiokafka `KafkaBroker` (special thanks to @spataphore1337):

```python
from faststream import FastStream
from faststream.kafka import KafkaBroker, TopicPartition

broker = KafkaBroker()

topic_partition_first = TopicPartition("my_topic", 1)
topic_partition_second = TopicPartition("my_topic", 2)

@broker.subscribe(partitions=[topic_partition_first, topic_partition_second])
async def some_consumer(msg):
   ...
```

* Update Release Notes for 0.5.4 by @faststream-release-notes-updater in [#1421](https://github.com/ag2ai/faststream/pull/1421){.external-link target="_blank"}
* feature: manual partition assignment to Kafka by [@spataphore1337](https://github.com/spataphore1337){.external-link target="_blank"} in [#1422](https://github.com/ag2ai/faststream/pull/1422){.external-link target="_blank"}
* Chore/update deps by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1429](https://github.com/ag2ai/faststream/pull/1429){.external-link target="_blank"}
* Fix/correct dynamic subscriber registration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1433](https://github.com/ag2ai/faststream/pull/1433){.external-link target="_blank"}
* chore: bump version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1435](https://github.com/ag2ai/faststream/pull/1435){.external-link target="_blank"}


**Full Changelog**: [#0.5.4...0.5.5](https://github.com/ag2ai/faststream/compare/0.5.4...0.5.5){.external-link target="_blank"}

## 0.5.4

### What's Changed

* Update Release Notes for 0.5.3 by @faststream-release-notes-updater in [#1400](https://github.com/ag2ai/faststream/pull/1400){.external-link target="_blank"}
* fix (#1415): raise SetupError if rpc and reply_to are using in TestCL… by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1419](https://github.com/ag2ai/faststream/pull/1419){.external-link target="_blank"}
* Chore/update deps2 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1418](https://github.com/ag2ai/faststream/pull/1418){.external-link target="_blank"}
* refactor: correct security with kwarg params merging by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1417](https://github.com/ag2ai/faststream/pull/1417){.external-link target="_blank"}
* fix (#1414): correct Message.ack error processing by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1420](https://github.com/ag2ai/faststream/pull/1420){.external-link target="_blank"}

**Full Changelog**: [#0.5.3...0.5.4](https://github.com/ag2ai/faststream/compare/0.5.3...0.5.4){.external-link target="_blank"}

## 0.5.3

### What's Changed
* Update Release Notes for 0.5.2 by @faststream-release-notes-updater in [#1382](https://github.com/ag2ai/faststream/pull/1382){.external-link target="_blank"}
* Fix/setup at broker connection instead of starting by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1385](https://github.com/ag2ai/faststream/pull/1385){.external-link target="_blank"}
* Tests/add path tests by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1388](https://github.com/ag2ai/faststream/pull/1388){.external-link target="_blank"}
* Fix/path with router prefix by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1395](https://github.com/ag2ai/faststream/pull/1395){.external-link target="_blank"}
* chore: update dependencies by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1396](https://github.com/ag2ai/faststream/pull/1396){.external-link target="_blank"}
* chore: bump version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1397](https://github.com/ag2ai/faststream/pull/1397){.external-link target="_blank"}
* chore: polishing by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1399](https://github.com/ag2ai/faststream/pull/1399){.external-link target="_blank"}


**Full Changelog**: [#0.5.2...0.5.3](https://github.com/ag2ai/faststream/compare/0.5.2...0.5.3){.external-link target="_blank"}

## 0.5.2

### What's Changed

Just a little bugfix patch. Fixes #1379 and #1376.

* Update Release Notes for 0.5.1 by @faststream-release-notes-updater in [#1378](https://github.com/ag2ai/faststream/pull/1378){.external-link target="_blank"}
* Tests/fastapi background by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1380](https://github.com/ag2ai/faststream/pull/1380){.external-link target="_blank"}
* Fix/0.5.2 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1381](https://github.com/ag2ai/faststream/pull/1381){.external-link target="_blank"}


**Full Changelog**: [#0.5.1...0.5.2](https://github.com/ag2ai/faststream/compare/0.5.1...0.5.2){.external-link target="_blank"}

## 0.5.1

### What's Changed

We already have some fixes related to `RedisBroker` (#1375, #1376) and some new features for you:

1. Now `broke.include_router(...)` allows to pass some arguments to setup router at including moment instead of creation

  ```python
  broker.include_router(
     router,
     prefix="test_",
     dependencies=[Depends(...)],
     middlewares=[BrokerMiddleware],
     include_in_schema=False,
  )
  ```

2. `KafkaBroker().subscriber(...)` now consumes `aiokafka.ConsumerRebalanceListener` object.
You can find more information about it in the official [**aiokafka** doc](https://aiokafka.readthedocs.io/en/stable/consumer.html?highlight=subscribe#topic-subscription-by-pattern)

  (close #1319)

  ```python
  broker = KafkaBroker()

  broker.subscriber(..., listener=MyRebalancer())
  ```

`pattern` option was added too, but it is still experimental and does not support `Path`

3. [`Path`](https://faststream.airt.ai/latest/nats/message/#subject-pattern-access) feature performance was increased. Also, `Path` is suitable for NATS `PullSub` batch subscription as well now.

```python
from faststream import NatsBroker, PullSub

broker = NatsBroker()

@broker.subscriber(
    "logs.{level}",
    steam="test-stream",
    pull_sub=PullSub(batch=True),
)
async def base_handler(
    ...,
    level: str = Path(),
):
  ...
```

* Update Release Notes for 0.5.0 by @faststream-release-notes-updater in https://github.com/ag2ai/faststream/pull/1366
* chore: bump version by @Lancetnik in https://github.com/ag2ai/faststream/pull/1372
* feat: kafka listener, extended include_router by @Lancetnik in https://github.com/ag2ai/faststream/pull/1374
* Fix/1375 by @Lancetnik in https://github.com/ag2ai/faststream/pull/1377


**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.0...0.5.1

## 0.5.0

### What's Changed

This is the biggest change since the creation of FastStream. We have completely refactored the entire package, changing the object registration mechanism, message processing pipeline, and application lifecycle. However, you won't even notice it—we've preserved all public APIs from breaking changes. The only feature not compatible with the previous code is the new middleware.

New features:

1. `await FastStream.stop()` method and `StopApplication` exception to stop a `FastStream` worker are added.

2. `broker.subscriber()` and `router.subscriber()` functions now return a `Subscriber` object you can use later.

```python
subscriber = broker.subscriber("test")

@subscriber(filter = lambda msg: msg.content_type == "application/json")
async def handler(msg: dict[str, Any]):
    ...

@subscriber()
async def handler(msg: dict[str, Any]):
    ...
 ```

This is the preferred syntax for [filtering](https://faststream.airt.ai/latest/getting-started/subscription/filtering/) now (the old one will be removed in `0.6.0`)

 3. The `router.publisher()` function now returns the correct `Publisher` object you can use later (after broker startup).

 ```python
 publisher = router.publisher("test")

 @router.subscriber("in")
 async def handler():
     await publisher.publish("msg")
 ```

 (Until `0.5.0` you could use it in this way with `broker.publisher` only)

 4. A list of `middlewares` can be passed to a `broker.publisher` as well:

 ```python
 broker = Broker(..., middlewares=())

 @broker.subscriber(..., middlewares=())
 @broker.publisher(..., middlewares=())  # new feature
 async def handler():
     ...
 ```

5. Broker-level middlewares now affect all ways to publish a message, so you can encode application outgoing messages here.

6. ⚠️ BREAKING CHANGE ⚠️ : both `subscriber` and `publisher` middlewares should be async context manager type

```python
async def subscriber_middleware(call_next, msg):
    return await call_next(msg)

async def publisher_middleware(call_next, msg, **kwargs):
    return await call_next(msg, **kwargs)

@broker.subscriber(
    "in",
    middlewares=(subscriber_middleware,),
)
@broker.publisher(
    "out",
    middlewares=(publisher_middleware,),
)
async def handler(msg):
    return msg
```

Such changes allow you two previously unavailable features:
* suppress any exceptions and pass fall-back message body to publishers, and
* patch any outgoing message headers and other parameters.

Without those features we could not implement [Observability Middleware](https://github.com/ag2ai/faststream/issues/916) or any similar tool, so it is the job that just had to be done.
7. A better **FastAPI** compatibility: `fastapi.BackgroundTasks` and `response_class` subscriber option are supported.

8. All `.pyi` files are removed, and explicit docstrings and methods options are added.

9. New subscribers can be registered in runtime (with an already-started broker):

```python
subscriber = broker.subscriber("dynamic")
subscriber(handler_method)
...
broker.setup_subscriber(subscriber)
await subscriber.start()
...
await subscriber.close()
```

10. `faststream[docs]` distribution is removed.

* Update Release Notes for 0.4.7 by @faststream-release-notes-updater in https://github.com/ag2ai/faststream/pull/1295
* 1129 - Create a publish command for the CLI by @MRLab12 in https://github.com/ag2ai/faststream/pull/1151
* Chore: packages upgraded by @davorrunje in https://github.com/ag2ai/faststream/pull/1306
* docs: fix typos by @omahs in https://github.com/ag2ai/faststream/pull/1309
* chore: update dependencies by @Lancetnik in https://github.com/ag2ai/faststream/pull/1323
* docs: fix misc by @Lancetnik in https://github.com/ag2ai/faststream/pull/1324
* docs (#1327): correct RMQ exchanges behavior by @Lancetnik in https://github.com/ag2ai/faststream/pull/1328
* fix: typer 0.12 exclude by @Lancetnik in https://github.com/ag2ai/faststream/pull/1341
* 0.5.0 by @Lancetnik in https://github.com/ag2ai/faststream/pull/1326
  * close #1103
  * close #840
  * fix #690
  * fix #1206
  * fix #1227
  * close #568
  * close #1303
  * close #1287
  * feat #607
* Generate docs and linter fixes by @davorrunje in https://github.com/ag2ai/faststream/pull/1348
* Fix types by @davorrunje in https://github.com/ag2ai/faststream/pull/1349
* chore: update dependencies by @Lancetnik in https://github.com/ag2ai/faststream/pull/1358
* feat: final middlewares by @Lancetnik in https://github.com/ag2ai/faststream/pull/1357
* Docs/0.5.0 features by @Lancetnik in https://github.com/ag2ai/faststream/pull/1360

### New Contributors
* @MRLab12 made their first contribution in https://github.com/ag2ai/faststream/pull/1151
* @omahs made their first contribution in https://github.com/ag2ai/faststream/pull/1309

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.4.7...0.5.0

## 0.5.0rc2

### What's Changed

This is the final API change before stable `0.5.0` release

⚠️ HAS BREAKING CHANGE

In it, we stabilize the behavior of publishers & subscribers middlewares

```python
async def subscriber_middleware(call_next, msg):
    return await call_next(msg)

async def publisher_middleware(call_next, msg, **kwargs):
    return await call_next(msg, **kwargs)

@broker.subscriber(
    "in",
    middlewares=(subscriber_middleware,),
)
@broker.publisher(
    "out",
    middlewares=(publisher_middleware,),
)
async def handler(msg):
    return msg
```

Such changes allows you two features previously unavailable

* suppress any exceptions and pas fall-back message body to publishers
* patch any outgoing message headers and other parameters

Without these features we just can't implement [Observability Middleware](https://github.com/ag2ai/faststream/issues/916) or any similar tool, so it is the job to be done.

Now you are free to get access at any message processing stage and we are one step closer to the framework we would like to create!

* Update Release Notes for 0.5.0rc0 by @faststream-release-notes-updater in https://github.com/ag2ai/faststream/pull/1347
* Generate docs and linter fixes by @davorrunje in https://github.com/ag2ai/faststream/pull/1348
* Fix types by @davorrunje in https://github.com/ag2ai/faststream/pull/1349
* chore: update dependencies by @Lancetnik in https://github.com/ag2ai/faststream/pull/1358
* feat: final middlewares by @Lancetnik in https://github.com/ag2ai/faststream/pull/1357


**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.5.0rc0...0.5.0rc2

## 0.5.0rc0

### What's Changed

This is the biggest change since the creation of FastStream. We have completely refactored the entire package, changing the object registration mechanism, message processing pipeline, and application lifecycle. However, you won't even notice it—we've preserved all public APIs from breaking changes. The only feature not compatible with the previous code is the new middleware.

This is still an RC (Release Candidate) for you to test before the stable release. You can manually install it in your project:

```console
pip install faststream==0.5.0rc0
```

We look forward to your feedback!

New features:

1. `await FastStream.stop()` method and `StopApplication` exception to stop a `FastStream` worker are added.

2. `broker.subscriber()` and `router.subscriber()` functions now return a `Subscriber` object you can use later.

```python
subscriber = broker.subscriber("test")

@subscriber(filter = lambda msg: msg.content_type == "application/json")
async def handler(msg: dict[str, Any]):
    ...

@subscriber()
async def handler(msg: dict[str, Any]):
    ...
 ```

This is the preferred syntax for [filtering](https://faststream.airt.ai/latest/getting-started/subscription/filtering/) now (the old one will be removed in `0.6.0`)

 3. The `router.publisher()` function now returns the correct `Publisher` object you can use later (after broker startup).

 ```python
 publisher = router.publisher("test")

 @router.subscriber("in")
 async def handler():
     await publisher.publish("msg")
 ```

 (Until `0.5.0` you could use it in this way with `broker.publisher` only)

 4. A list of `middlewares` can be passed to a `broker.publisher` as well:

 ```python
 broker = Broker(..., middlewares=())

 @broker.subscriber(..., middlewares=())
 @broker.publisher(..., middlewares=())  # new feature
 async def handler():
     ...
 ```

5. Broker-level middlewares now affect all ways to publish a message, so you can encode application outgoing messages here.

6. ⚠️ BREAKING CHANGE ⚠️ : both `subscriber` and `publisher` middlewares should be async context manager type

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def subscriber_middleware(msg_body):
    yield msg_body

@asynccontextmanager
async def publisher_middleware(
    msg_to_publish,
    **publish_arguments,
):
    yield msg_to_publish

@broker.subscriber("in", middlewares=(subscriber_middleware,))
@broker.publisher("out", middlewares=(publisher_middleware,))
async def handler():
    ...
```

7. A better **FastAPI** compatibility: `fastapi.BackgroundTasks` and `response_class` subscriber option are supported.

8. All `.pyi` files are removed, and explicit docstrings and methods options are added.

9. New subscribers can be registered in runtime (with an already-started broker):

```python
subscriber = broker.subscriber("dynamic")
subscriber(handler_method)
...
broker.setup_subscriber(subscriber)
await subscriber.start()
...
await subscriber.close()
```

10. `faststream[docs]` distribution is removed.

* Update Release Notes for 0.4.7 by @faststream-release-notes-updater in https://github.com/ag2ai/faststream/pull/1295
* 1129 - Create a publish command for the CLI by @MRLab12 in https://github.com/ag2ai/faststream/pull/1151
* Chore: packages upgraded by @davorrunje in https://github.com/ag2ai/faststream/pull/1306
* docs: fix typos by @omahs in https://github.com/ag2ai/faststream/pull/1309
* chore: update dependencies by @Lancetnik in https://github.com/ag2ai/faststream/pull/1323
* docs: fix misc by @Lancetnik in https://github.com/ag2ai/faststream/pull/1324
* docs (#1327): correct RMQ exchanges behavior by @Lancetnik in https://github.com/ag2ai/faststream/pull/1328
* fix: typer 0.12 exclude by @Lancetnik in https://github.com/ag2ai/faststream/pull/1341
* 0.5.0 by @Lancetnik in https://github.com/ag2ai/faststream/pull/1326
* close #1103
* close #840
* fix #690
* fix #1206
* fix #1227
* close #568
* close #1303
* close #1287
* feat #607

### New Contributors

* @MRLab12 made their first contribution in https://github.com/ag2ai/faststream/pull/1151
* @omahs made their first contribution in https://github.com/ag2ai/faststream/pull/1309

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.4.7...0.5.0rc0


## 0.4.7

### What's Changed

* Update Release Notes for 0.4.6 by @faststream-release-notes-updater in [#1286](https://github.com/ag2ai/faststream/pull/1286){.external-link target="_blank"}
* fix (#1263): correct nested discriminator msg type AsyncAPI schema by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1288](https://github.com/ag2ai/faststream/pull/1288){.external-link target="_blank"}
* docs: add `apply_types` warning notice to subscription/index.md by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1291](https://github.com/ag2ai/faststream/pull/1291){.external-link target="_blank"}
* chore: fixed nats-py version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1294](https://github.com/ag2ai/faststream/pull/1294){.external-link target="_blank"}

**Full Changelog**: [#0.4.6...0.4.7](https://github.com/ag2ai/faststream/compare/0.4.6...0.4.7){.external-link target="_blank"}

## 0.4.6

### What's Changed
* Add poll in confluent producer to fix BufferError by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1277](https://github.com/ag2ai/faststream/pull/1277){.external-link target="_blank"}
* Cover confluent asyncapi tests by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1279](https://github.com/ag2ai/faststream/pull/1279){.external-link target="_blank"}
* chore: bump package versions by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1285](https://github.com/ag2ai/faststream/pull/1285){.external-link target="_blank"}


**Full Changelog**: [#0.4.5...0.4.6](https://github.com/ag2ai/faststream/compare/0.4.5...0.4.6){.external-link target="_blank"}

## 0.4.5

### What's Changed
* Update Release Notes for 0.4.4 by @faststream-release-notes-updater in [#1260](https://github.com/ag2ai/faststream/pull/1260){.external-link target="_blank"}
* Removed unused pytest dependency from redis/schemas.py by [@ashambalev](https://github.com/ashambalev){.external-link target="_blank"} in [#1261](https://github.com/ag2ai/faststream/pull/1261){.external-link target="_blank"}
* chore: bumped package versions by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1270](https://github.com/ag2ai/faststream/pull/1270){.external-link target="_blank"}
* fix (#1263): correct AsyncAPI schema in discriminator case by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1272](https://github.com/ag2ai/faststream/pull/1272){.external-link target="_blank"}

### New Contributors
* [@ashambalev](https://github.com/ashambalev){.external-link target="_blank"} made their first contribution in [#1261](https://github.com/ag2ai/faststream/pull/1261){.external-link target="_blank"}

**Full Changelog**: [#0.4.4...0.4.5](https://github.com/ag2ai/faststream/compare/0.4.4...0.4.5){.external-link target="_blank"}

## 0.4.4

### What's Changed

Add RedisStream batch size option

```python
@broker.subscriber(stream=StreamSub("input", batch=True, max_records=3))
async def on_input_data(msgs: list[str]):
    assert len(msgs) <= 3
```

* Update Release Notes for 0.4.3 by @faststream-release-notes-updater in [#1247](https://github.com/ag2ai/faststream/pull/1247){.external-link target="_blank"}
* docs: add manual run section by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1249](https://github.com/ag2ai/faststream/pull/1249){.external-link target="_blank"}
* feat (#1252): respect Redis StreamSub last_id with consumer group by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1256](https://github.com/ag2ai/faststream/pull/1256){.external-link target="_blank"}
* fix: correct Redis consumer group behavior by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1258](https://github.com/ag2ai/faststream/pull/1258){.external-link target="_blank"}
* feat: add Redis Stream max_records option by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1259](https://github.com/ag2ai/faststream/pull/1259){.external-link target="_blank"}


**Full Changelog**: [#0.4.3...0.4.4](https://github.com/ag2ai/faststream/compare/0.4.3...0.4.4){.external-link target="_blank"}

## 0.4.3

### What's Changed

Allow to specify **Redis Stream** maxlen option in publisher:

```python
@broker.publisher(stream=StreamSub("Output", max_len=10))
async def on_input_data():
    ....
```

* chore: bump version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1198](https://github.com/ag2ai/faststream/pull/1198){.external-link target="_blank"}
* Update Release Notes for 0.4.2 by @faststream-release-notes-updater in [#1199](https://github.com/ag2ai/faststream/pull/1199){.external-link target="_blank"}
* Add missing API documentation for apply_pattern by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1201](https://github.com/ag2ai/faststream/pull/1201){.external-link target="_blank"}
* chore: polishing by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1203](https://github.com/ag2ai/faststream/pull/1203){.external-link target="_blank"}
* Comment out retry and timeout in a confluent test by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1207](https://github.com/ag2ai/faststream/pull/1207){.external-link target="_blank"}
* Commit offsets only if auto_commit is True by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1208](https://github.com/ag2ai/faststream/pull/1208){.external-link target="_blank"}
* Add a CI job to check for missed docs changes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1217](https://github.com/ag2ai/faststream/pull/1217){.external-link target="_blank"}
* fix: inconsistent NATS publisher signature by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1218](https://github.com/ag2ai/faststream/pull/1218){.external-link target="_blank"}
* Upgrade packages by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1226](https://github.com/ag2ai/faststream/pull/1226){.external-link target="_blank"}
* chore: bump dawidd6/action-download-artifact from 3.0.0 to 3.1.1 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1239](https://github.com/ag2ai/faststream/pull/1239){.external-link target="_blank"}
* chore: bump dependencies by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1246](https://github.com/ag2ai/faststream/pull/1246){.external-link target="_blank"}
* feat (#1235): StreamSub maxlen parameter by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1245](https://github.com/ag2ai/faststream/pull/1245){.external-link target="_blank"}
* fix (#1234): correct FastAPI path passing, fix typehints by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1236](https://github.com/ag2ai/faststream/pull/1236){.external-link target="_blank"}
* fix (#1231): close RMQ while reconnecting by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1238](https://github.com/ag2ai/faststream/pull/1238){.external-link target="_blank"}


**Full Changelog**: [#0.4.2...0.4.3](https://github.com/ag2ai/faststream/compare/0.4.2...0.4.3){.external-link target="_blank"}

## 0.4.2

### What's Changed

#### Bug fixes

* fix: correct RMQ Topic testing routing by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1196](https://github.com/ag2ai/faststream/pull/1196){.external-link target="_blank"}
* fix #1191: correct RMQ ssl default port by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1195](https://github.com/ag2ai/faststream/pull/1195){.external-link target="_blank"}
* fix #1143: ignore Depends in AsyncAPI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1197](https://github.com/ag2ai/faststream/pull/1197){.external-link target="_blank"}


**Full Changelog**: [#0.4.1...0.4.2](https://github.com/ag2ai/faststream/compare/0.4.1...0.4.2){.external-link target="_blank"}

## 0.4.1

### What's Changed

#### Bug fixes

* Fix: use FastAPI overrides in subscribers by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1189](https://github.com/ag2ai/faststream/pull/1189){.external-link target="_blank"}
* Handle confluent consumer commit failure by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1193](https://github.com/ag2ai/faststream/pull/1193){.external-link target="_blank"}

#### Documentation

* Include Confluent in home and features pages by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1186](https://github.com/ag2ai/faststream/pull/1186){.external-link target="_blank"}
* Use pydantic model for publishing in docs example by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1187](https://github.com/ag2ai/faststream/pull/1187){.external-link target="_blank"}


**Full Changelog**: [#0.4.0...0.4.1](https://github.com/ag2ai/faststream/compare/0.4.0...0.4.1){.external-link target="_blank"}

## 0.4.0

### What's Changed

This release adds support for the [Confluent's Python Client for Apache Kafka (TM)](https://github.com/confluentinc/confluent-kafka-python). Confluent's Python Client for Apache Kafka does not support natively `async` functions and its integration with modern async-based services is a bit trickier. That was the reason why our initial supported by Kafka broker used [aiokafka](https://github.com/aio-libs/aiokafka). However, that choice was a less fortunate one as it is as well maintained as the Confluent version. After receiving numerous requests, we finally decided to bite the bullet and create an `async` wrapper around Confluent's Python Client and add full support for it in FastStream.

If you want to try it out, install it first with:
```sh
pip install "faststream[confluent]>=0.4.0"
```

To connect to Kafka using the FastStream KafkaBroker module, follow these steps:

1. Initialize the KafkaBroker instance: Start by initializing a KafkaBroker instance with the necessary configuration, including Kafka broker address.

2. Create your processing logic: Write a function that will consume the incoming messages in the defined format and produce a response to the defined topic

3. Decorate your processing function: To connect your processing function to the desired Kafka topics you need to decorate it with `#!python @broker.subscriber(...)` and `#!python @broker.publisher(...)` decorators. Now, after you start your application, your processing function will be called whenever a new message in the subscribed topic is available and produce the function return value to the topic defined in the publisher decorator.

Here's a simplified code example demonstrating how to establish a connection to Kafka using FastStream's KafkaBroker module:

```python
from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.subscriber("in-topic")
@broker.publisher("out-topic")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
```

For more information, please visit the documentation at:

https://faststream.airt.ai/latest/confluent/

#### List of Changes

* Update Release Notes for 0.3.13 by @faststream-release-notes-updater in https://github.com/ag2ai/faststream/pull/1119
* docs: close #1125 by @Lancetnik in https://github.com/ag2ai/faststream/pull/1126
* Add support for confluent python lib by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1042
* Update tutorial docs to include confluent code examples by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1131
* Add installation instructions for confluent by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1132
* Update Release Notes for 0.4.0rc0 by @faststream-release-notes-updater in https://github.com/ag2ai/faststream/pull/1130
* chore: remove useless branch from CI by @Lancetnik in https://github.com/ag2ai/faststream/pull/1135
* chore: bump mkdocs-git-revision-date-localized-plugin from 1.2.1 to 1.2.2 by @dependabot in https://github.com/ag2ai/faststream/pull/1140
* chore: strict fast-depends version by @Lancetnik in https://github.com/ag2ai/faststream/pull/1145
* chore: update copyright by @Lancetnik in https://github.com/ag2ai/faststream/pull/1144
* fix: correct Windows shutdown by @Lancetnik in https://github.com/ag2ai/faststream/pull/1148
* docs: fix typo by @saroz014 in https://github.com/ag2ai/faststream/pull/1154
* Middleware Document Syntax Error by @SepehrBazyar in https://github.com/ag2ai/faststream/pull/1156
* fix: correct FastAPI Context type hints by @Lancetnik in https://github.com/ag2ai/faststream/pull/1155
* Fix bug which results in lost confluent coverage report by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1160
* Fix failing ack tests for confluent by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1166
* Update version to 0.4.0 and update docs by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1171
* feat #1180: add StreamRouter.on_broker_shutdown hook by @Lancetnik in https://github.com/ag2ai/faststream/pull/1182
* Fix bug - using old upload-artifact version by @kumaranvpl in https://github.com/ag2ai/faststream/pull/1183
* Release 0.4.0 by @davorrunje in https://github.com/ag2ai/faststream/pull/1184

### New Contributors
* @saroz014 made their first contribution in https://github.com/ag2ai/faststream/pull/1154

**Full Changelog**: https://github.com/ag2ai/faststream/compare/0.3.13...0.4.0

## 0.4.0rc0

### What's Changed

This is a **preview version** of 0.4.0 release introducing support for Confluent-based Kafka broker.

Here's a simplified code example demonstrating how to establish a connection to Kafka using FastStream's KafkaBroker module:
```python
from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.subscriber("in-topic")
@broker.publisher("out-topic")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
```

#### Changes

* Add support for confluent python lib by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1042](https://github.com/ag2ai/faststream/pull/1042){.external-link target="_blank"}


**Full Changelog**: [#0.3.13...0.4.0rc0](https://github.com/ag2ai/faststream/compare/0.3.13...0.4.0rc0){.external-link target="_blank"}

## 0.3.13

### What's Changed

#### New features

* New shutdown logic by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1117](https://github.com/ag2ai/faststream/pull/1117){.external-link target="_blank"}

#### Bug fixes

* Fix minor typos in documentation and code  by [@mj0nez](https://github.com/mj0nez){.external-link target="_blank"} in [#1116](https://github.com/ag2ai/faststream/pull/1116){.external-link target="_blank"}

### New Contributors
* [@mj0nez](https://github.com/mj0nez){.external-link target="_blank"} made their first contribution in [#1116](https://github.com/ag2ai/faststream/pull/1116){.external-link target="_blank"}

**Full Changelog**: [#0.3.12...0.3.13](https://github.com/ag2ai/faststream/compare/0.3.12...0.3.13){.external-link target="_blank"}

## 0.3.12

### What's Changed

#### Bug fixes

* fix (#1110): correct RMQ Topic pattern test publish by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1112](https://github.com/ag2ai/faststream/pull/1112){.external-link target="_blank"}

#### Misc

* chore: upgraded packages, black replaced with ruff format by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1097](https://github.com/ag2ai/faststream/pull/1097){.external-link target="_blank"}
* chore: upgraded packages by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1111](https://github.com/ag2ai/faststream/pull/1111){.external-link target="_blank"}


**Full Changelog**: [#0.3.11...0.3.12](https://github.com/ag2ai/faststream/compare/0.3.11...0.3.12){.external-link target="_blank"}

## 0.3.11

### What's Changed

NATS concurrent subscriber:

By default,  NATS subscriber consumes messages with a block per subject. So, you can't process multiple messages from the same subject at the same time. But, with the `broker.subscriber(..., max_workers=...)` option, you can! It creates an async tasks pool to consume multiple messages from the same subject and allows you to process them concurrently!

```python
from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream()

@broker.subscriber("test-subject", max_workers=10)
async def handler(...):
   """Can process up to 10 messages in the same time."""
```

* Update Release Notes for 0.3.10 by @faststream-release-notes-updater in [#1091](https://github.com/ag2ai/faststream/pull/1091){.external-link target="_blank"}
* fix (#1100): FastAPI 0.106 compatibility by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1102](https://github.com/ag2ai/faststream/pull/1102){.external-link target="_blank"}

**Full Changelog**: [#0.3.10...0.3.11](https://github.com/ag2ai/faststream/compare/0.3.10...0.3.11){.external-link target="_blank"}

## 0.3.10

### What's Changed

#### New features

* feat: Context initial option by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1086](https://github.com/ag2ai/faststream/pull/1086){.external-link target="_blank"}

#### Bug fixes

* fix (#1087): add app_dir option to docs serve/gen commands by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1088](https://github.com/ag2ai/faststream/pull/1088){.external-link target="_blank"}

#### Documentation

* docs: add Context initial section by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1089](https://github.com/ag2ai/faststream/pull/1089){.external-link target="_blank"}

#### Other

* chore: linting by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1081](https://github.com/ag2ai/faststream/pull/1081){.external-link target="_blank"}
* chore: delete accidentally added .bak file by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1085](https://github.com/ag2ai/faststream/pull/1085){.external-link target="_blank"}

**Full Changelog**: [#0.3.9...0.3.10](https://github.com/ag2ai/faststream/compare/0.3.9...0.3.10){.external-link target="_blank"}

## 0.3.9

### What's Changed

#### Bug fixes:

* fix (#1082): correct NatsTestClient stream publisher by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1083](https://github.com/ag2ai/faststream/pull/1083){.external-link target="_blank"}

#### Chore:

* chore: adding pragmas for detect-secrets by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1080](https://github.com/ag2ai/faststream/pull/1080){.external-link target="_blank"}


**Full Changelog**: [#0.3.8...0.3.9](https://github.com/ag2ai/faststream/compare/0.3.8...0.3.9){.external-link target="_blank"}

## 0.3.8

### What's Changed

* bug: Fix `faststream.redis.fastapi.RedisRouter` stream and list subscription
* bug: Fix `TestNatsClient` with `batch=True`
* chore: add citation file by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1061](https://github.com/ag2ai/faststream/pull/1061){.external-link target="_blank"}
* docs: remove pragma comments by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1063](https://github.com/ag2ai/faststream/pull/1063){.external-link target="_blank"}
* docs: update README by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1064](https://github.com/ag2ai/faststream/pull/1064){.external-link target="_blank"}
* chore: increase rate limit and max connections by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1070](https://github.com/ag2ai/faststream/pull/1070){.external-link target="_blank"}
* chore: packages updated by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1076](https://github.com/ag2ai/faststream/pull/1076){.external-link target="_blank"}
* tests (#570): cover docs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1077](https://github.com/ag2ai/faststream/pull/1077){.external-link target="_blank"}

**Full Changelog**: [#0.3.7...0.3.8](https://github.com/ag2ai/faststream/compare/0.3.7...0.3.8){.external-link target="_blank"}

## 0.3.7

### What's Changed

* feat (#974): add FastAPI Context by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1060](https://github.com/ag2ai/faststream/pull/1060){.external-link target="_blank"}
* chore: update pre-commit by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1058](https://github.com/ag2ai/faststream/pull/1058){.external-link target="_blank"}

Support regular FastStream Context with FastAPI plugin

```python
from fastapi import FastAPI
from faststream.redis.fastapi import RedisRouter, Logger

router = RedisRouter()

@router.subscriber("test")
async def handler(msg, logger: Logger):
    logger.info(msg)

app = FastAPI(lifespan=router.lifespan_context)
app.include_router(router)
```

**Full Changelog**: [#0.3.6...0.3.7](https://github.com/ag2ai/faststream/compare/0.3.6...0.3.7){.external-link target="_blank"}

## 0.3.6

### What's Changed

* chore: correct update release CI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1050](https://github.com/ag2ai/faststream/pull/1050){.external-link target="_blank"}
* Update Release Notes for main by [@faststream](https://github.com/faststream){.external-link target="_blank"}-release-notes-updater in [#1051](https://github.com/ag2ai/faststream/pull/1051){.external-link target="_blank"}
* chore: fix building docs script by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1055](https://github.com/ag2ai/faststream/pull/1055){.external-link target="_blank"}
* 0.3.6 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1056](https://github.com/ag2ai/faststream/pull/1056){.external-link target="_blank"}
  * bug: remove `packaging` dependency
  * bug: correct **FastAPI** batch consuming
  * docs: add search meta to all pages
  * docs: polish all pages styles, fix typos
  * chore: add ruff rule to check print

**Full Changelog**: [#0.3.5...0.3.6](https://github.com/ag2ai/faststream/compare/0.3.5...0.3.6){.external-link target="_blank"}

## 0.3.5

### What's Changed

A large update by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1048](https://github.com/ag2ai/faststream/pull/1048){.external-link target="_blank"}

Provides with the ability to setup `graceful_timeout` to wait for consumed messages processed correctly before application shutdown - `#!python Broker(graceful_timeout=30.0)` (waits up to `#!python 30` seconds)

* allows to get access to `#!python context.get_local("message")` from **FastAPI** plugin
* docs: fix Avro custom serialization example
* docs: add KafkaBroker `publish_batch` notice
* docs: add RabbitMQ security page
* fix: respect retry attempts with `NackMessage` exception
* test Kafka nack and reject behavior
* bug: fix import error with anyio version 4.x by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1049](https://github.com/ag2ai/faststream/pull/1049){.external-link target="_blank"}

**Full Changelog**: [#0.3.4...0.3.5](https://github.com/ag2ai/faststream/compare/0.3.4...0.3.5){.external-link target="_blank"}

## 0.3.4

### What's Changed

#### Features:

* feat: add support for anyio 4.x by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1044](https://github.com/ag2ai/faststream/pull/1044){.external-link target="_blank"}

#### Documentation

* docs: add multiple FastAPI routers section by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1041](https://github.com/ag2ai/faststream/pull/1041){.external-link target="_blank"}

#### Chore

* chore: updated release notes by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1040](https://github.com/ag2ai/faststream/pull/1040){.external-link target="_blank"}
* chore: use Github App to generate token for release notes PR by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1043](https://github.com/ag2ai/faststream/pull/1043){.external-link target="_blank"}

**Full Changelog**: [#0.3.3...0.3.4](https://github.com/ag2ai/faststream/compare/0.3.3...0.3.4){.external-link target="_blank"}

## 0.3.3

### What's Changed

Features:

* feat: add support for Python 3.12 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1034](https://github.com/ag2ai/faststream/pull/1034){.external-link target="_blank"}

Chores:

* chore: updated release notes and upgraded packages by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1029](https://github.com/ag2ai/faststream/pull/1029){.external-link target="_blank"}

**Full Changelog**: [#0.3.2...0.3.3](https://github.com/ag2ai/faststream/compare/0.3.2...0.3.3){.external-link target="_blank"}

## 0.3.2

### What's Changed

#### New features:

* feat: add Redis security configuration by [@sternakt](https://github.com/sternakt){.external-link target="_blank"} and [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1025](https://github.com/ag2ai/faststream/pull/1025){.external-link target="_blank"}
* feat: add list of Messages NATS PullSub by [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} in [#1023](https://github.com/ag2ai/faststream/pull/1023){.external-link target="_blank"}

#### Chore:

* chore: polishing by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1016](https://github.com/ag2ai/faststream/pull/1016){.external-link target="_blank"}
* chore: update release notes by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1017](https://github.com/ag2ai/faststream/pull/1017){.external-link target="_blank"}
* chore: bump pytest-asyncio from 0.21.1 to 0.23.2 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1019](https://github.com/ag2ai/faststream/pull/1019){.external-link target="_blank"}
* chore: bump semgrep from 1.50.0 to 1.51.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1018](https://github.com/ag2ai/faststream/pull/1018){.external-link target="_blank"}
* chore: add pull_request permission to workflow by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1022](https://github.com/ag2ai/faststream/pull/1022){.external-link target="_blank"}


**Full Changelog**: [#0.3.1...0.3.2](https://github.com/ag2ai/faststream/compare/0.3.1...0.3.2){.external-link target="_blank"}

## 0.3.1

### What's Changed

Features:

* feat: added reply-to delivery mode for RabbitMQ by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1015](https://github.com/ag2ai/faststream/pull/1015){.external-link target="_blank"}

Bug fixes:

* fix: non-payload information injected included in AsyncAPI docs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1015](https://github.com/ag2ai/faststream/pull/1015){.external-link target="_blank"}

Documentation:

* docs: fix misspelled FastDepends reference in README.md by @spectacularfailure in [#1013](https://github.com/ag2ai/faststream/pull/1013){.external-link target="_blank"}

### New Contributors

* @spectacularfailure made their first contribution in [#1013](https://github.com/ag2ai/faststream/pull/1013){.external-link target="_blank"}

**Full Changelog**: [#0.3.0...0.3.1](https://github.com/ag2ai/faststream/compare/0.3.0...0.3.1){.external-link target="_blank"}

## 0.3.0

### What's Changed

The main feature of the 0.3.0 release is added Redis support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1003](https://github.com/ag2ai/faststream/pull/1003){.external-link target="_blank"}

You can install it by the following command:

```bash
pip install "faststream[redis]"
```

Here is a little code example

```python
from faststream import FastStream, Logger
from faststream.redis import RedisBroker

broker = RedisBroker()
app = FastStream(broker)

@broker.subscriber(
    channel="test",  # or
    # list="test",     or
    # stream="test",
)
async def handle(msg: str, logger: Logger):
    logger.info(msg)
```

#### Other features

* feat: show reload directories with `--reload` flag by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#981](https://github.com/ag2ai/faststream/pull/981){.external-link target="_blank"}
* feat: implement validate and no_ack subscriber options (#926) by [@mihail8531](https://github.com/mihail8531){.external-link target="_blank"} in [#988](https://github.com/ag2ai/faststream/pull/988){.external-link target="_blank"}
* other features by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1003](https://github.com/ag2ai/faststream/pull/1003){.external-link target="_blank"}
    * Improve error logs (missing CLI arguments, undefined starting)
    * Add `faststream docs serve --reload ...` option for documentation hotreload
    * Add `faststream run --reload-extension .env` option to watch by changes in such files
    * Support `faststream run -k 1 -k 2 ...` as `k=["1", "2"]` extra options
    * Add subscriber, publisher and router `include_in_schema: bool` argument to disable **AsyncAPI** render
    * remove `watchfiles` from default distribution
    * Allow create `#!python broker.publisher(...)` with already running broker
    * **FastAPI**-like lifespan `FastStream` application context manager
    * automatic `TestBroker(connect_only=...)` argument based on AST
    * add `NatsMessage.in_progress()` method

#### Testing

* test: improve coverage by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#983](https://github.com/ag2ai/faststream/pull/983){.external-link target="_blank"}

#### Documentation

* docs: fix module name in NATS example by [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} in [#993](https://github.com/ag2ai/faststream/pull/993){.external-link target="_blank"}
* docs: Update docs to add  how to customize asyncapi docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#999](https://github.com/ag2ai/faststream/pull/999){.external-link target="_blank"}
* docs: polish Redis pages by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1005](https://github.com/ag2ai/faststream/pull/1005){.external-link target="_blank"}
* docs: bump docs to the new taskiq-faststream version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1009](https://github.com/ag2ai/faststream/pull/1009){.external-link target="_blank"}

#### Chore

* chore: add broken link checker by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#985](https://github.com/ag2ai/faststream/pull/985){.external-link target="_blank"}
* chore: disable verbose in check broken links workflow by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#986](https://github.com/ag2ai/faststream/pull/986){.external-link target="_blank"}
* chore: add left out md files to fix broken links by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#987](https://github.com/ag2ai/faststream/pull/987){.external-link target="_blank"}
* chore: update mike workflow to use config by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#982](https://github.com/ag2ai/faststream/pull/982){.external-link target="_blank"}
* chore: add workflow to update release notes automatically by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#992](https://github.com/ag2ai/faststream/pull/992){.external-link target="_blank"}
* chore: pip packages version updated by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#998](https://github.com/ag2ai/faststream/pull/998){.external-link target="_blank"}
* chore: create PR to merge updated release notes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1004](https://github.com/ag2ai/faststream/pull/1004){.external-link target="_blank"}

### New Contributors
* [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} made their first contribution in [#993](https://github.com/ag2ai/faststream/pull/993){.external-link target="_blank"}
* [@mihail8531](https://github.com/mihail8531){.external-link target="_blank"} made their first contribution in [#988](https://github.com/ag2ai/faststream/pull/988){.external-link target="_blank"}

**Full Changelog**: [#0.2.15...0.3.0](https://github.com/ag2ai/faststream/compare/0.2.15...0.3.0){.external-link target="_blank"}

## 0.3.0rc0

### What's Changed

The main feature of the 0.3.x release is added Redis support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1003](https://github.com/ag2ai/faststream/pull/1003){.external-link target="_blank"}

You can install it manually:

```bash
pip install faststream==0.3.0rc0 && pip install "faststream[redis]"
```

#### Other features

* feat: show reload directories with `--reload` flag by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#981](https://github.com/ag2ai/faststream/pull/981){.external-link target="_blank"}
* Improve error logs (missing CLI arguments, undefined starting)
* Add `faststream docs serve --reload ...` option for documentation hotreload
* Add `faststream run --reload-extension .env` option to watch by changes in such files
* Support `faststream run -k 1 -k 2 ...` as `k=["1", "2"]` extra options
* Add subscriber, publisher and router `include_in_schema: bool` argument to disable **AsyncAPI** render
* remove `watchfiles` from default distribution
* Allow create `#!python @broker.publisher(...)` with already running broker
* **FastAPI**-like lifespan `FastStream` application context manager
* automatic `TestBroker(connect_only=...)` argument based on AST
* add `NatsMessage.in_progress()` method

#### Testing

* test: improve coverage by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#983](https://github.com/ag2ai/faststream/pull/983){.external-link target="_blank"}

#### Documentation

* docs: fix module name in NATS example by [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} in [#993](https://github.com/ag2ai/faststream/pull/993){.external-link target="_blank"}
* docs: Update docs to add  how to customize asyncapi docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#999](https://github.com/ag2ai/faststream/pull/999){.external-link target="_blank"}

#### Chore

* chore: add broken link checker by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#985](https://github.com/ag2ai/faststream/pull/985){.external-link target="_blank"}
* chore: disable verbose in check broken links workflow by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#986](https://github.com/ag2ai/faststream/pull/986){.external-link target="_blank"}
* chore: add left out md files to fix broken links by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#987](https://github.com/ag2ai/faststream/pull/987){.external-link target="_blank"}
* chore: update mike workflow to use config by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#982](https://github.com/ag2ai/faststream/pull/982){.external-link target="_blank"}
* chore: add workflow to update release notes automatically by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#992](https://github.com/ag2ai/faststream/pull/992){.external-link target="_blank"}
* chore: pip packages version updated by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#998](https://github.com/ag2ai/faststream/pull/998){.external-link target="_blank"}

### New Contributors

* [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} made their first contribution in [#993](https://github.com/ag2ai/faststream/pull/993){.external-link target="_blank"}

**Full Changelog**: [#0.2.15...0.3.0rc0](https://github.com/ag2ai/faststream/compare/0.2.15...0.3.0rc0){.external-link target="_blank"}

## 0.2.15

### What's Changed

#### Bug fixes

* fix (#972): correct Context default behavior by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/973](https://github.com/ag2ai/faststream/pull/973){.external-link target="_blank"}
* fix: correct CLI run by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/978](https://github.com/ag2ai/faststream/pull/978){.external-link target="_blank"}

#### Documentation

* docs: update readme docs link by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/966](https://github.com/ag2ai/faststream/pull/966){.external-link target="_blank"}
* docs: add a new landing page for docs by [@harishmohanraj](https://github.com/harishmohanraj){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/954](https://github.com/ag2ai/faststream/pull/954){.external-link target="_blank"}
* docs: Fix broken internal links by [@harishmohanraj](https://github.com/harishmohanraj){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/976](https://github.com/ag2ai/faststream/pull/976){.external-link target="_blank"}
* docs: use mkdocs footer by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/977](https://github.com/ag2ai/faststream/pull/977){.external-link target="_blank"}

#### Misc

* test (#957): add AsyncAPI FastAPI security test by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/958](https://github.com/ag2ai/faststream/pull/958){.external-link target="_blank"}
* test: update tests for cli utils functions by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/960](https://github.com/ag2ai/faststream/pull/960){.external-link target="_blank"}
* chore: update release notes for version 0.2.14 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/961](https://github.com/ag2ai/faststream/pull/961){.external-link target="_blank"}
* chore: Add back deleted index file for API Reference by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/963](https://github.com/ag2ai/faststream/pull/963){.external-link target="_blank"}
* chore: bump dirty-equals from 0.6.0 to 0.7.1.post0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/970](https://github.com/ag2ai/faststream/pull/970){.external-link target="_blank"}
* chore: bump semgrep from 1.48.0 to 1.50.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/968](https://github.com/ag2ai/faststream/pull/968){.external-link target="_blank"}
* chore: bump mkdocs-glightbox from 0.3.4 to 0.3.5 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/967](https://github.com/ag2ai/faststream/pull/967){.external-link target="_blank"}
* chore: bump mkdocs-material from 9.4.8 to 9.4.10 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/971](https://github.com/ag2ai/faststream/pull/971){.external-link target="_blank"}
* chore: bump ruff from 0.1.5 to 0.1.6 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/969](https://github.com/ag2ai/faststream/pull/969){.external-link target="_blank"}


**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.14...0.2.15](https://github.com/ag2ai/faststream/compare/0.2.14...0.2.15){.external-link target="_blank"}

## 0.2.14

### What's Changed

#### Bug fixes

* fix: usage pass apps module rather than file path by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/955](https://github.com/ag2ai/faststream/pull/955){.external-link target="_blank"}
* fix: trigger docs deployment by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/944](https://github.com/ag2ai/faststream/pull/944){.external-link target="_blank"}

#### Documentation

* docs: reduce built docs size by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/952](https://github.com/ag2ai/faststream/pull/952){.external-link target="_blank"}
* docs: fix update_release script by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/945](https://github.com/ag2ai/faststream/pull/945){.external-link target="_blank"}

#### Misc

* chore: polishing by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/946](https://github.com/ag2ai/faststream/pull/946){.external-link target="_blank"}
* сhore: add manual publish btn to CI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/950](https://github.com/ag2ai/faststream/pull/950){.external-link target="_blank"}
* chore: limit open dev dependency versions by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/953](https://github.com/ag2ai/faststream/pull/953){.external-link target="_blank"}


**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.13...0.2.14](https://github.com/ag2ai/faststream/compare/0.2.13...0.2.14){.external-link target="_blank"}


## 0.2.13

### What's Changed

* chore: Remove uvloop python 3.12 restriction from pyproject by [@sternakt](https://github.com/sternakt){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/914](https://github.com/ag2ai/faststream/pull/914){.external-link target="_blank"}
* fix: mike deploy command by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/919](https://github.com/ag2ai/faststream/pull/919){.external-link target="_blank"}
* chore: update dependencies by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/920](https://github.com/ag2ai/faststream/pull/920){.external-link target="_blank"}
* chore: use dev dependencies to build docs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/921](https://github.com/ag2ai/faststream/pull/921){.external-link target="_blank"}
* chore: update packages' versions by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/937](https://github.com/ag2ai/faststream/pull/937){.external-link target="_blank"}
* fix: FastAPI subscriber Path support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/931](https://github.com/ag2ai/faststream/pull/931){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.12...0.2.13](https://github.com/ag2ai/faststream/compare/0.2.12...0.2.13){.external-link target="_blank"}

## 0.2.12

### What's Changed
* feat: NATS polling subscriber by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/912](https://github.com/ag2ai/faststream/pull/912){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.11...0.2.12](https://github.com/ag2ai/faststream/compare/0.2.11...0.2.12){.external-link target="_blank"}

## 0.2.11

### What's Changed

#### Bug fixes

* fix (#910): correct pydantic enum refs resolving by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/911](https://github.com/ag2ai/faststream/pull/911){.external-link target="_blank"}

#### Documentation

* docs: update the number of lines of code referred to in the documentation by [@vvanglro](https://github.com/vvanglro){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/905](https://github.com/ag2ai/faststream/pull/905){.external-link target="_blank"}
* docs: add API reference in docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/891](https://github.com/ag2ai/faststream/pull/891){.external-link target="_blank"}
* docs: add release notes for version 0.2.10 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/907](https://github.com/ag2ai/faststream/pull/907){.external-link target="_blank"}
* docs: detail 0.2.10 release note by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/908](https://github.com/ag2ai/faststream/pull/908){.external-link target="_blank"}
* docs: proofread and update 0.2.10 release notes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/909](https://github.com/ag2ai/faststream/pull/909){.external-link target="_blank"}

### New Contributors
* [@vvanglro](https://github.com/vvanglro){.external-link target="_blank"} made their first contribution in [https://github.com/ag2ai/faststream/pull/905](https://github.com/ag2ai/faststream/pull/905){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.10...0.2.11](https://github.com/ag2ai/faststream/compare/0.2.10...0.2.11){.external-link target="_blank"}

* fix (#910): correct pydantic enum refs resolving by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/911](https://github.com/ag2ai/faststream/pull/911){.external-link target="_blank"}

#### Documentation

* docs: update the number of lines of code referred to in the documentation by [@vvanglro](https://github.com/vvanglro){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/905](https://github.com/ag2ai/faststream/pull/905){.external-link target="_blank"}
* docs: add API reference in docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/891](https://github.com/ag2ai/faststream/pull/891){.external-link target="_blank"}
* docs: add release notes for version 0.2.10 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/907](https://github.com/ag2ai/faststream/pull/907){.external-link target="_blank"}
* docs: detail 0.2.10 release note by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/908](https://github.com/ag2ai/faststream/pull/908){.external-link target="_blank"}
* docs: proofread and update 0.2.10 release notes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/909](https://github.com/ag2ai/faststream/pull/909){.external-link target="_blank"}

### New Contributors
* [@vvanglro](https://github.com/vvanglro){.external-link target="_blank"} made their first contribution in [https://github.com/ag2ai/faststream/pull/905](https://github.com/ag2ai/faststream/pull/905){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.10...0.2.11](https://github.com/ag2ai/faststream/compare/0.2.10...0.2.11){.external-link target="_blank"}

## 0.2.10

### What's Changed

Now, you can hide your connection secrets in the **AsyncAPI** schema by manually setting up the server URL:

```python
broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",  # Connection URL
    asyncapi_url="amqp://****:****@localhost:5672/",  # Public schema URL
)
```

Additionally, the **RabbitMQ AsyncAPI** schema has been improved, adding support for `faststream.security`, and the connection scheme is now defined automatically.

**RabbitMQ** connection parameters are now merged, allowing you to define the main connection data as a URL string and customize it using kwargs:

```python
broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    host="127.0.0.1",
)

# amqp://guest:guest@127.0.0.1:5672/ - The final URL
```
* A more suitable `faststream.security` import instead of `faststream.broker.security`
* chore: add release notes for 0.2.9 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/894](https://github.com/ag2ai/faststream/pull/894){.external-link target="_blank"}
* chore: upgrade packages by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/901](https://github.com/ag2ai/faststream/pull/901){.external-link target="_blank"}
* chore: use js redirect and redirect to version by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/902](https://github.com/ag2ai/faststream/pull/902){.external-link target="_blank"}
* feat: add `asyncapi_url` broker arg by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/903](https://github.com/ag2ai/faststream/pull/903){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.9...0.2.10](https://github.com/ag2ai/faststream/compare/0.2.9...0.2.10){.external-link target="_blank"}

## 0.2.9

### What's Changed
* docs: fix grammatical errors in README.md by [@JanumalaAkhilendra](https://github.com/JanumalaAkhilendra){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/880](https://github.com/ag2ai/faststream/pull/880){.external-link target="_blank"}
* chore: update release notes by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/881](https://github.com/ag2ai/faststream/pull/881){.external-link target="_blank"}
* docs: use meta tag for redirect by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/886](https://github.com/ag2ai/faststream/pull/886){.external-link target="_blank"}
* chore: semgrep upgrade by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/888](https://github.com/ag2ai/faststream/pull/888){.external-link target="_blank"}
* docs: update README.md by [@bhargavshirin](https://github.com/bhargavshirin){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/889](https://github.com/ag2ai/faststream/pull/889){.external-link target="_blank"}
* fix (#892): use normalized subjects in NATS streams by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/893](https://github.com/ag2ai/faststream/pull/893){.external-link target="_blank"}

### New Contributors
* [@JanumalaAkhilendra](https://github.com/JanumalaAkhilendra){.external-link target="_blank"} made their first contribution in [https://github.com/ag2ai/faststream/pull/880](https://github.com/ag2ai/faststream/pull/880){.external-link target="_blank"}
* [@bhargavshirin](https://github.com/bhargavshirin){.external-link target="_blank"} made their first contribution in [https://github.com/ag2ai/faststream/pull/889](https://github.com/ag2ai/faststream/pull/889){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.8...0.2.9](https://github.com/ag2ai/faststream/compare/0.2.8...0.2.9){.external-link target="_blank"}

## 0.2.8

### What's Changed
* fix: FASTAPI_V2 always True by [@shepilov](https://github.com/shepilov){.external-link target="_blank"}-vladislav in [https://github.com/ag2ai/faststream/pull/877](https://github.com/ag2ai/faststream/pull/877){.external-link target="_blank"}
* feat: better RMQ AsyncAPI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/879](https://github.com/ag2ai/faststream/pull/879){.external-link target="_blank"}

### New Contributors
* [@shepilov](https://github.com/shepilov){.external-link target="_blank"}-vladislav made their first contribution in [https://github.com/ag2ai/faststream/pull/877](https://github.com/ag2ai/faststream/pull/877){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.7...0.2.8](https://github.com/ag2ai/faststream/compare/0.2.7...0.2.8){.external-link target="_blank"}


## 0.2.7

### What's Changed
* fix: ImportError: typing 'override' from 'faststream._compat' (python 3.12) by [@Jaroslav2001](https://github.com/Jaroslav2001){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/870](https://github.com/ag2ai/faststream/pull/870){.external-link target="_blank"}
* fix: remove jsonref dependency by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/873](https://github.com/ag2ai/faststream/pull/873){.external-link target="_blank"}


**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.6...0.2.7](https://github.com/ag2ai/faststream/compare/0.2.6...0.2.7){.external-link target="_blank"}

## 0.2.6

### What's Changed
* docs: add avro encoding, decoding examples by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/844](https://github.com/ag2ai/faststream/pull/844){.external-link target="_blank"}
* docs: fix typo in README.md by [@omimakhare](https://github.com/omimakhare){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/849](https://github.com/ag2ai/faststream/pull/849){.external-link target="_blank"}
* fix: update mypy, semgrep versions and fix arg-type mypy error by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/851](https://github.com/ag2ai/faststream/pull/851){.external-link target="_blank"}
* docs: fix typo by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/859](https://github.com/ag2ai/faststream/pull/859){.external-link target="_blank"}
* docs: detail Release Notes by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/855](https://github.com/ag2ai/faststream/pull/855){.external-link target="_blank"}
* docs: write documentation for kafka security by [@sternakt](https://github.com/sternakt){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/860](https://github.com/ag2ai/faststream/pull/860){.external-link target="_blank"}
* docs: asyncapi tool config added by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/861](https://github.com/ag2ai/faststream/pull/861){.external-link target="_blank"}
* docs: retain GET params while redirecting by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/862](https://github.com/ag2ai/faststream/pull/862){.external-link target="_blank"}
* docs: add article for using FastStream with Django by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/864](https://github.com/ag2ai/faststream/pull/864){.external-link target="_blank"}
* chore: discord invite link changed by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/863](https://github.com/ag2ai/faststream/pull/863){.external-link target="_blank"}
* docs: add some Django integration details by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/866](https://github.com/ag2ai/faststream/pull/866){.external-link target="_blank"}
* fix: remove pydantic defs  in AsyncAPI schema by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/869](https://github.com/ag2ai/faststream/pull/869){.external-link target="_blank"}

### New Contributors
* [@omimakhare](https://github.com/omimakhare){.external-link target="_blank"} made their first contribution in [https://github.com/ag2ai/faststream/pull/849](https://github.com/ag2ai/faststream/pull/849){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.5...0.2.6](https://github.com/ag2ai/faststream/compare/0.2.5...0.2.6){.external-link target="_blank"}

## 0.2.5

### What's Changed

* fix: pass missing parameters and update docs by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/841](https://github.com/ag2ai/faststream/pull/841){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.4...0.2.5](https://github.com/ag2ai/faststream/compare/0.2.4...0.2.5){.external-link target="_blank"}

## 0.2.4

### New Functionalities

Now, `Context` provides access to inner [dict keys too](./getting-started/context.md#existing_fields):

```python
# headers is a `dict`
async def handler(
  user_id: int = Context("message.headers.user_id", cast=True),
): ...
```

Added `Header` object as a shortcut to `#!python Context("message.headers.")` inner fields (**NATS** [example](./nats/message.md#headers-access)):

```python
# the same with the previous example
async def handler(
  user_id: int = Header(),
  u_id: int = Header("user_id"),  # with custom name
): ...
```

Added `Path` object to get access to [**NATS** wildcard](./nats/message.md#subject-pattern-access) subject or [**RabbitMQ** topic](./rabbit/message.md#topic-pattern-access) routing key (a shortcut to access `#!python Context("message.path.")` as well):

```python
@nats_broker.subscriber("logs.{level}")
async def handler(
  level: str = Path(),
)
```

Also, the original message `Context` annotation was copied from `faststream.[broker].annotations.[Broker]Message` to `faststream.[broker].[Broker]Message` to provide you with faster access to the most commonly used object (**NATS** [example](./nats/message.md#message-access)).

### What's Changed

* Remove faststream_gen docs and remove code to generate faststream_gen docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/824](https://github.com/ag2ai/faststream/pull/824){.external-link target="_blank"}
* Update docs article to use cookiecutter template by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/828](https://github.com/ag2ai/faststream/pull/828){.external-link target="_blank"}
* Split real broker tests to independent runs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/825](https://github.com/ag2ai/faststream/pull/825){.external-link target="_blank"}
* Remove unused docs/docs_src/kafka examples and its tests by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/829](https://github.com/ag2ai/faststream/pull/829){.external-link target="_blank"}
* Run docs deployment only for specific file changes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/830](https://github.com/ag2ai/faststream/pull/830){.external-link target="_blank"}
* Fix formatting in deploy docs workflow by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/833](https://github.com/ag2ai/faststream/pull/833){.external-link target="_blank"}
* Path operations by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/823](https://github.com/ag2ai/faststream/pull/823){.external-link target="_blank"}
* Mypy error fixed for uvloop by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/839](https://github.com/ag2ai/faststream/pull/839){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.3...0.2.4](https://github.com/ag2ai/faststream/compare/0.2.3...0.2.4){.external-link target="_blank"}

## 0.2.3

### What's Changed

* Fix: disable test features with TestClient by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/813](https://github.com/ag2ai/faststream/pull/813){.external-link target="_blank"}
* New AsyncAPI naming by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/735](https://github.com/ag2ai/faststream/pull/735){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.2...0.2.3](https://github.com/ag2ai/faststream/compare/0.2.2...0.2.3){.external-link target="_blank"}

## 0.2.2

### What's Changed

* Adds specific mypy ignore comment by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/803](https://github.com/ag2ai/faststream/pull/803){.external-link target="_blank"}
* Adds redirect template with mike by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/808](https://github.com/ag2ai/faststream/pull/808){.external-link target="_blank"}
* Adds google analytics script to redirect template by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/809](https://github.com/ag2ai/faststream/pull/809){.external-link target="_blank"}
* Adds conditional import of uvloop for Python versions less than 3.12 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/798](https://github.com/ag2ai/faststream/pull/798){.external-link target="_blank"}
* Adds missing nats imports by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/795](https://github.com/ag2ai/faststream/pull/795){.external-link target="_blank"}
* Adds Kafka acknowledgement by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/793](https://github.com/ag2ai/faststream/pull/793){.external-link target="_blank"}

### New Contributors

* [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} made their first contribution in [https://github.com/ag2ai/faststream/pull/795](https://github.com/ag2ai/faststream/pull/795){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.1...0.2.2](https://github.com/ag2ai/faststream/compare/0.2.1...0.2.2){.external-link target="_blank"}

## 0.2.1

### What's Changed

* Add custom 404 error page by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/792](https://github.com/ag2ai/faststream/pull/792){.external-link target="_blank"}
* Add README NATS mention by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/788](https://github.com/ag2ai/faststream/pull/788){.external-link target="_blank"}
* Conditional import of uvloop for Python versions less than 3.12 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/798](https://github.com/ag2ai/faststream/pull/798){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.2.0...0.2.1](https://github.com/ag2ai/faststream/compare/0.2.0...0.2.1){.external-link target="_blank"}

## 0.2.0

### What's Changed

* Add comprehensive guide on how to use faststream template by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/772](https://github.com/ag2ai/faststream/pull/772){.external-link target="_blank"}
* Open external links in new tab by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/774](https://github.com/ag2ai/faststream/pull/774){.external-link target="_blank"}
* Publish docs for minor version not for every patch by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/777](https://github.com/ag2ai/faststream/pull/777){.external-link target="_blank"}
* Complete Kafka part of faststream docs by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/775](https://github.com/ag2ai/faststream/pull/775){.external-link target="_blank"}
* Bump semgrep from 1.41.0 to 1.42.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/787](https://github.com/ag2ai/faststream/pull/787){.external-link target="_blank"}
* Add 0.2.0 NATS support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/692](https://github.com/ag2ai/faststream/pull/692){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.1.6...0.2.0](https://github.com/ag2ai/faststream/compare/0.1.6...0.2.0){.external-link target="_blank"}

## 0.1.6

### What's Changed

* Add coverage badge at docs index by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/762](https://github.com/ag2ai/faststream/pull/762){.external-link target="_blank"}
* Fill asyncapi custom information page by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/767](https://github.com/ag2ai/faststream/pull/767){.external-link target="_blank"}
* Add article for using faststream template by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/768](https://github.com/ag2ai/faststream/pull/768){.external-link target="_blank"}
* Use httpx instead of requests by [@rjambrecic](https://github.com/rjambrecic){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/771](https://github.com/ag2ai/faststream/pull/771){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.1.5...0.1.6](https://github.com/ag2ai/faststream/compare/0.1.5...0.1.6){.external-link target="_blank"}

## 0.1.4

### What's Changed

* tiny typo by [@julzhk](https://github.com/julzhk){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/740](https://github.com/ag2ai/faststream/pull/740){.external-link target="_blank"}
* docs: add docs mention by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/744](https://github.com/ag2ai/faststream/pull/744){.external-link target="_blank"}
* Add code of conduct and include badge for it in README by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/747](https://github.com/ag2ai/faststream/pull/747){.external-link target="_blank"}
* Fixed docs building when pydantic version less than 2.4.0 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/748](https://github.com/ag2ai/faststream/pull/748){.external-link target="_blank"}
* fix: raise inner exceptions in `with_real` tests by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/751](https://github.com/ag2ai/faststream/pull/751){.external-link target="_blank"}
* docs fix by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/752](https://github.com/ag2ai/faststream/pull/752){.external-link target="_blank"}
* Bugfixes 745 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/749](https://github.com/ag2ai/faststream/pull/749){.external-link target="_blank"}

### New Contributors

* [@julzhk](https://github.com/julzhk){.external-link target="_blank"} made their first contribution in [https://github.com/ag2ai/faststream/pull/740](https://github.com/ag2ai/faststream/pull/740){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.1.3...0.1.4](https://github.com/ag2ai/faststream/compare/0.1.3...0.1.4){.external-link target="_blank"}

## 0.1.3

### What's Changed

* docs: fix styles by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/717](https://github.com/ag2ai/faststream/pull/717){.external-link target="_blank"}
* test (#638): extra AsyncAPI channel naming test by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/719](https://github.com/ag2ai/faststream/pull/719){.external-link target="_blank"}
* test: cover docs_src/context by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/723](https://github.com/ag2ai/faststream/pull/723){.external-link target="_blank"}
* library to framework changed by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/724](https://github.com/ag2ai/faststream/pull/724){.external-link target="_blank"}
* Create templates for issues and pull requests by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/727](https://github.com/ag2ai/faststream/pull/727){.external-link target="_blank"}
* Bump actions/dependency-review-action from 2 to 3 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/728](https://github.com/ag2ai/faststream/pull/728){.external-link target="_blank"}
* Bump actions/cache from 2 to 3 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/729](https://github.com/ag2ai/faststream/pull/729){.external-link target="_blank"}
* Bump semgrep from 1.40.0 to 1.41.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/732](https://github.com/ag2ai/faststream/pull/732){.external-link target="_blank"}
* Bump ruff from 0.0.290 to 0.0.291 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/733](https://github.com/ag2ai/faststream/pull/733){.external-link target="_blank"}
* Polish contributing file and remove duplicate docker compose file by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/734](https://github.com/ag2ai/faststream/pull/734){.external-link target="_blank"}
* Bump dawidd6/action-download-artifact from 2.26.0 to 2.28.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/731](https://github.com/ag2ai/faststream/pull/731){.external-link target="_blank"}
* Bump actions/checkout from 3 to 4 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/730](https://github.com/ag2ai/faststream/pull/730){.external-link target="_blank"}
* Pydantic2.4.0 compat by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/738](https://github.com/ag2ai/faststream/pull/738){.external-link target="_blank"}
* fix: add url option to _connection_args by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/739](https://github.com/ag2ai/faststream/pull/739){.external-link target="_blank"}
* Fix typos and grammar in Kafka and RabbitMQ articles in the docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/736](https://github.com/ag2ai/faststream/pull/736){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/compare/0.1.1...0.1.3](https://github.com/ag2ai/faststream/compare/0.1.1...0.1.3){.external-link target="_blank"}

## 0.1.1

### What's Changed

* Bump ruff from 0.0.289 to 0.0.290 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/672](https://github.com/ag2ai/faststream/pull/672){.external-link target="_blank"}
* Make docs port configurable in serve-docs.sh by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/675](https://github.com/ag2ai/faststream/pull/675){.external-link target="_blank"}
* Fix docs img by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/673](https://github.com/ag2ai/faststream/pull/673){.external-link target="_blank"}
* Added release notes by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/679](https://github.com/ag2ai/faststream/pull/679){.external-link target="_blank"}
* Fix typos, grammar mistakes in index and README by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/681](https://github.com/ag2ai/faststream/pull/681){.external-link target="_blank"}
* Add smokeshow workflow to update coverage badge by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/687](https://github.com/ag2ai/faststream/pull/687){.external-link target="_blank"}
* fix: correct rmq delayed handler router registration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/691](https://github.com/ag2ai/faststream/pull/691){.external-link target="_blank"}
* Add faststream-gen section and crypto tutorial in Getting started by [@rjambrecic](https://github.com/rjambrecic){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/689](https://github.com/ag2ai/faststream/pull/689){.external-link target="_blank"}
* Fix typos and grammar mistakes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/699](https://github.com/ag2ai/faststream/pull/699){.external-link target="_blank"}
* fix: correct StreamRouter broker annotation by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/700](https://github.com/ag2ai/faststream/pull/700){.external-link target="_blank"}
* typos fixed by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/701](https://github.com/ag2ai/faststream/pull/701){.external-link target="_blank"}
* Add faststream-gen section inside the README.md by [@rjambrecic](https://github.com/rjambrecic){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/707](https://github.com/ag2ai/faststream/pull/707){.external-link target="_blank"}
* Fix broken links in README file by [@harishmohanraj](https://github.com/harishmohanraj){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/706](https://github.com/ag2ai/faststream/pull/706){.external-link target="_blank"}
* publish to PyPi added to CI by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/710](https://github.com/ag2ai/faststream/pull/710){.external-link target="_blank"}
* Fix example and async docs images by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/713](https://github.com/ag2ai/faststream/pull/713){.external-link target="_blank"}
* 696 add example to faststream gen examples which uses datetime attribute by [@rjambrecic](https://github.com/rjambrecic){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/714](https://github.com/ag2ai/faststream/pull/714){.external-link target="_blank"}
* release 0.1.1 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/ag2ai/faststream/pull/715](https://github.com/ag2ai/faststream/pull/715){.external-link target="_blank"}

**Full Changelog**: [https://github.com/ag2ai/faststream/commits/0.1.1](https://github.com/ag2ai/faststream/commits/0.1.1){.external-link target="_blank"}

## 0.1.0

**FastStream** is a new package based on the ideas and experiences gained from [FastKafka](https://github.com/airtai/fastkafka){.external-link target="_blank"} and [Propan](https://github.com/lancetnik/propan){.external-link target="_blank"}. By joining our forces, we picked up the best from both packages and created the unified way to write services capable of processing streamed data regardless of the underlying protocol. We'll continue to maintain both packages, but new development will be in this project. If you are starting a new service, this package is the recommended way to do it.

### Features

[**FastStream**](https://faststream.airt.ai/latest/) simplifies the process of writing producers and consumers for message queues, handling all the
parsing, networking and documentation generation automatically.

Making streaming microservices has never been easier. Designed with junior developers in mind, **FastStream** simplifies your work while keeping the door open for more advanced use-cases. Here's a look at the core features that make **FastStream** a go-to framework for modern, data-centric microservices.

* **Multiple Brokers**: **FastStream** provides a unified API to work across multiple message brokers (**Kafka**, **RabbitMQ** support)

* [**Pydantic Validation**](./faststream.md/#writing-app-code): Leverage [**Pydantic's**](https://docs.pydantic.dev/){.external-link target="_blank"} validation capabilities to serialize and validates incoming messages

* [**Automatic Docs**](./faststream.md/#project-documentation): Stay ahead with automatic [AsyncAPI](https://www.asyncapi.com/){.external-link target="_blank"} documentation.

* **Intuitive**: full typed editor support makes your development experience smooth, catching errors before they reach runtime

* [**Powerful Dependency Injection System**](./faststream.md/#dependencies): Manage your service dependencies efficiently with **FastStream**'s built-in DI system.

* [**Testable**](./faststream.md/#testing-the-service): supports in-memory tests, making your CI/CD pipeline faster and more reliable

* **Extendable**: use extensions for lifespans, custom serialization and middlewares

* [**Integrations**](./faststream.md/#any-framework): **FastStream** is fully compatible with any HTTP framework you want ([**FastAPI**](./faststream.md/#fastapi-plugin) especially)

* **Built for Automatic Code Generation**: **FastStream** is optimized for automatic code generation using advanced models like GPT and Llama

That's **FastStream** in a nutshell—easy, efficient, and powerful. Whether you're just starting with streaming microservices or looking to scale, **FastStream** has got you covered.
