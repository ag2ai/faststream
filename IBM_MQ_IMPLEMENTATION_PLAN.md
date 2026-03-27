# IBM MQ Implementation Plan

Related issue: `#2807`

## Goal

Add first-class IBM MQ support to FastStream using:

- `mq-container` for local development and connected tests
- `ibmmq` (`mq-mqi-python`) for the Python runtime integration

The implementation should feel native to FastStream, follow the existing broker-provider architecture, and deliver a useful queue-based MVP before tackling IBM MQ topic/pubsub support.

## Success Criteria

The initial implementation is successful when all of the following are true:

1. Users can install `faststream[mq]` and import `MQBroker` from `faststream.mq`.
2. A FastStream app can publish to and consume from IBM MQ queues.
3. Handler return values can be sent as replies using IBM MQ request/reply semantics.
4. `MQMessage.ack()` and `MQMessage.nack()` work reliably via MQ transaction control.
5. FastStream test tooling supports an in-memory/fake IBM MQ broker.
6. Connected tests run against an IBM MQ container in CI.
7. Minimal docs, examples, telemetry providers, and AsyncAPI output exist for the provider.

## Guiding Principles

- Match existing provider structure rather than inventing a special-case integration.
- Treat IBM MQ as a queue-first broker for the MVP.
- Be explicit where IBM MQ semantics do not map cleanly to existing FastStream behavior.
- Keep local development and CI reproducible with pinned container and client-library versions.
- Prioritize fake test support early so the provider can join the existing test matrix smoothly.

## Recommended MVP Scope

### In Scope

- Queue publish
- Queue subscribe/consume
- Request/reply using dynamic reply queues
- Header/property mapping
- Manual `ack()` and `nack()`
- FastAPI integration
- Fake/in-memory testing support
- Connected tests against IBM MQ container
- Prometheus and OpenTelemetry settings providers
- Generic AsyncAPI support without IBM MQ-specific bindings

### Explicitly Out of Scope for MVP

- IBM MQ topic/pubsub support
- Batch publish/consume
- Automatic queue declaration as a user-facing broker feature
- Full DLQ/backout-queue automation
- Advanced SSL/OIDC/kerberos coverage in the first pass
- IBM MQ-specific AsyncAPI bindings or vendor extensions
- Cross-platform connected CI beyond the first stable Linux job

## High-Level Architecture

FastStream already provides the architectural pattern needed for IBM MQ support.

- Use `faststream/rabbit/` as the model for the user-facing queue API and provider layout.
- Use `faststream/confluent/` as the model for wrapping a synchronous client in an async FastStream runtime.
- Reuse the common broker, subscriber, publisher, router, and testing abstractions in `faststream/_internal/`.

IBM MQ should be implemented as a new first-class provider package, preferably `faststream/mq/`.

## Proposed Public API

### Package and Top-Level Exports

Create a new provider package:

```text
faststream/mq/
```

Top-level exports should mirror the style of the other providers:

- `MQBroker`
- `MQRouter`
- `MQRoute`
- `MQMessage`
- `MQResponse`
- `MQPublishMessage` or `MQPublishCommand` aliases if needed
- `TestMQBroker`

### Broker Constructor

The constructor should be IBM MQ-native rather than URL-first. Suggested shape:

```python
broker = MQBroker(
    queue_manager="QM1",
    channel="DEV.APP.SVRCONN",
    conn_name="127.0.0.1(1414)",
    username="app",
    password="password",
)
```

Recommended constructor parameters for MVP:

- `queue_manager`
- `channel`
- `conn_name`
- `host` / `port` convenience aliases if useful
- `username`
- `password`
- `security`
- `reply_model_queue` with default like `DEV.APP.MODEL.QUEUE`
- `wait_interval` for polling receive loops
- `transactional` / `ack_policy` related options if needed
- common FastStream options already supported by other brokers

### Subscriber and Publisher Shape

The MVP should use queue names as plain strings:

```python
@broker.subscriber("DEV.QUEUE.1")
@broker.publisher("DEV.QUEUE.2")
async def handle(msg: dict) -> dict:
    return {"ok": True, **msg}
```

An MQ-specific queue schema object can be added later if needed, but strings are enough for the first version.

## Proposed Package Layout

The provider should closely match the layout of the other built-in providers.

```text
faststream/mq/
  __init__.py
  annotations.py
  security.py
  message.py
  parser.py
  response.py
  testing.py
  broker/
    __init__.py
    broker.py
    registrator.py
    router.py
  configs/
    broker.py
    publisher.py
    subscriber.py
  helpers/
    client.py
    connection.py
    properties.py
  publisher/
    factory.py
    producer.py
    usecase.py
  subscriber/
    factory.py
    specification.py
    usecase.py
  fastapi/
    __init__.py
    fastapi.py
  opentelemetry/
    __init__.py
    provider.py
  prometheus/
    __init__.py
    provider.py
```

The exact filenames can be adjusted during implementation, but the overall shape should remain aligned with the rest of the codebase.

## Runtime Model

###+ Synchronous IBM MQ Client in an Async Framework

`ibmmq` is synchronous. The provider should not call it directly from the event loop.

Recommended design:

- Wrap blocking MQ operations in a dedicated single-thread executor per MQ connection.
- Follow the same general pattern used for the Confluent provider.
- Keep one producer connection and one consumer connection per subscriber worker in the MVP.
- Avoid sharing one queue manager handle across unrelated async tasks.

This design minimizes thread-safety surprises and keeps transaction boundaries predictable.

###+ Connection Ownership

Recommended connection layout:

- broker-level producer connection for publish and request calls
- one consumer connection per running subscriber
- no shared transaction state between subscriber workers

This is important because MQ `commit()` and `backout()` act on the unit of work owned by the queue manager handle.

###+ Subscriber Receive Loop

The subscriber receive loop should:

1. open the queue for input
2. call blocking `get()` in the executor
3. use `GMO_WAIT` with a short wait interval
4. continue polling until shutdown
5. parse the returned body, MQMD, and message properties into an `MQMessage`

Using a bounded wait interval is preferable to an uninterruptible long blocking call because it keeps shutdown responsive.

## Message Model

###+ Raw IBM MQ Artifacts to Preserve

The provider should preserve enough IBM MQ-native state on the raw message to support acknowledgements, observability, and custom use cases.

Recommended raw state to retain:

- queue manager handle/object
- queue handle/object
- MQMD
- GMO used for the receive
- message handle / properties handle when available
- raw message body bytes
- queue name
- optional reply queue manager name

###+ FastStream-Level Message Fields

`MQMessage` should expose the standard `StreamMessage` fields used by other providers:

- `body`
- `headers`
- `content_type`
- `correlation_id`
- `message_id`
- `reply_to`
- `raw_message`

Plus any MQ-specific fields that are helpful, such as:

- `reply_to_qmgr`
- `priority`
- `persistence`
- `expiry`
- `put_date` / `put_time` if practical

## Header and Property Mapping

IBM MQ gives us both MQMD fields and message properties. The provider should use them intentionally.

### Recommended Mapping Rules

- FastStream `headers` -> IBM MQ message properties
- FastStream `content-type` -> IBM MQ property named `content-type`
- FastStream `correlation_id` -> IBM MQ property `correlation_id` when explicitly set by FastStream
- transport-native `message_id` -> MQMD `MsgId`
- transport-native reply destination -> MQMD `ReplyToQ`
- optional reply queue manager -> MQMD `ReplyToQMgr`
- transport-native correlation for request/reply -> MQMD `CorrelId`

### Why This Split Matters

- MQMD fields should carry transport-level routing and correlation data.
- Message properties should carry FastStream-style logical headers.
- This keeps interoperability with non-FastStream IBM MQ clients realistic.

### Correlation ID Rule for MVP

Recommended behavior:

- if a FastStream header/property `correlation_id` exists, expose that as `MQMessage.correlation_id`
- otherwise fall back to a normalized representation of MQMD `CorrelId`

This allows FastStream tracing and middleware to work while preserving native IBM MQ request/reply behavior.

## Publish Semantics

The publish path should:

1. use FastStream's existing `encode_message` behavior
2. build an MQMD for transport-native fields
3. attach message properties for headers/content type/correlation metadata
4. write to the destination queue using `Queue.put`

The MVP should support:

- message body
- headers
- `correlation_id`
- `reply_to`
- `priority`
- `persistence`
- `expiry`

The MVP does not need batch publishing.

## Consume Semantics

The consume path should:

1. receive bytes + MQMD + properties
2. parse them into `MQMessage`
3. decode using the standard FastStream parser/decoder flow
4. commit or backout according to handler outcome and ack policy

Subscriber behavior should mirror FastStream conventions as closely as IBM MQ allows.

## Ack, Nack, and Reject Semantics

IBM MQ does not expose RabbitMQ-style ack/reject primitives. The equivalent control is transaction-based.

### Recommended MVP Mapping

- `ack()` -> `QueueManager.commit()`
- `nack()` -> `QueueManager.backout()`
- `reject()` -> commit and drop for MVP, with explicit documentation that IBM MQ has no broker-native reject-without-requeue primitive

### Important Caveat

For `ack()` and `nack()` to work correctly, messages must be received under syncpoint.

The subscriber receive path therefore needs to use syncpoint-aware `GMO` settings whenever the message may later be acked or nacked.

### Future Extension

In a later iteration, `reject()` could become configurable, for example:

- commit and drop
- commit and republish to a configured backout queue
- backout for redelivery

The MVP should keep the behavior simple and well documented.

## Request/Reply Design

IBM MQ request/reply should be implemented as a queue-based RPC pattern.

### Outbound Request Flow

1. create a dynamic reply queue from a configured model queue
2. set MQMD `ReplyToQ` to the dynamic queue name
3. send the request message
4. wait for a reply on the dynamic queue
5. match the response by native MQ correlation rules

### Correlation Strategy

Recommended approach:

- let the request message carry a deterministic `MsgId`
- require the response to set `CorrelId` to the original request `MsgId`
- also preserve FastStream's logical `correlation_id` header/property when available

### Handler Auto-Reply Behavior

When a subscriber returns a value and the inbound message has `ReplyToQ`:

- publish the response to that queue
- set response `CorrelId` to the request `MsgId`
- preserve logical headers where appropriate

### MVP Simplification

Use one dynamic reply queue per request call rather than building a shared reply listener pool. It is simpler and matches common IBM MQ request/reply examples.

## Topic Support Strategy

IBM MQ topics should not be part of the first implementation.

Reasoning:

- FastStream currently derives much of its test and API shape from queue-centric providers.
- IBM MQ topics introduce new objects and lifecycle decisions: topic strings, subscriptions, managed queues, and durable/non-durable behavior.
- Delivering stable queue support first reduces complexity and gets usable broker support into the project sooner.

After the queue MVP is stable, topic publish/subscribe can be implemented in a second phase using `Topic`, `Subscription`, and `SD` objects from `ibmmq`.

## Test Strategy

Testing must be built in layers so the provider becomes maintainable.

### 1. Fake Broker Support First

Add `TestMQBroker` early.

The fake broker should:

- route published messages directly to matching subscribers
- construct `MQMessage` instances that look like real provider messages
- support request/reply behavior
- preserve headers, correlation IDs, and reply targets
- allow the generic broker test suites to run without a real IBM MQ server

This should follow the pattern already used by `faststream/rabbit/testing.py` and `faststream/confluent/testing.py`.

### 2. Provider Test Modules

Add a new provider test package:

```text
tests/brokers/mq/
```

Expected files:

- `conftest.py`
- `basic.py`
- `test_connect.py`
- `test_publish.py`
- `test_consume.py`
- `test_requests.py`
- `test_router.py`
- `test_test_client.py`

Also add:

- `tests/prometheus/mq/`
- `tests/opentelemetry/mq/`
- `tests/asyncapi/mq/`
- `tests/mypy/mq.py`

### 3. Connected Test Queue Management

Unlike RabbitMQ and Redis, IBM MQ usually expects queues to exist before use.

To keep the existing test style with random queue names, the connected test fixtures should create and clean up temporary queues using IBM MQ PCF commands over the admin connection.

Recommended approach:

- use `DEV.ADMIN.SVRCONN` with admin credentials in test fixtures
- create a random local queue before each connected test or test class
- delete it during teardown

This keeps provider runtime code focused on messaging and avoids making queue declaration a public FastStream feature.

### 4. Connected Test Coverage

Minimum connected coverage should include:

- broker connect/disconnect
- publish/consume round trip
- request/reply round trip
- header/property round trip
- manual `ack()` commit
- manual `nack()` backout
- router integration
- fake broker compatibility
- telemetry provider coverage

### 5. Test Execution Strategy

Run MQ connected tests serially at first.

Reasoning:

- IBM MQ setup is heavier than the other dev brokers
- transaction boundaries are connection-specific
- queue creation/deletion can race under high parallelism

Once stable, selective parallelization can be revisited.

## Local Development Environment

Use the IBM MQ container project as the reference development environment.

### Recommended Container Baseline

- pin a specific IBM MQ image version
- set `LICENSE=accept`
- set `MQ_QMGR_NAME=QM1`
- expose port `1414`
- enable only the minimum extra services needed for development/tests

### Useful Dev Defaults Already Available

The developer image already provides useful defaults such as:

- `DEV.QUEUE.1`
- `DEV.QUEUE.2`
- `DEV.QUEUE.3`
- `DEV.DEAD.LETTER.QUEUE`
- `DEV.APP.MODEL.QUEUE`
- `DEV.APP.SVRCONN`
- `DEV.ADMIN.SVRCONN`

### Custom MQSC for Tests

Add custom MQSC for deterministic test setup when needed.

Recommended uses:

- extra model queues if needed
- predictable backout/dead-letter queues
- permissions for app/admin users
- any provider-specific test objects

## CI and Dependency Strategy

IBM MQ support has one major operational constraint: `ibmmq` requires IBM MQ runtime/client libraries.

### Recommended CI Strategy

- add a dedicated IBM MQ job rather than extending the general broker matrix immediately
- install the IBM MQ client libraries in that job or use a dedicated test image that already contains them
- start the IBM MQ container inside that job
- run the MQ-specific tests there

### Why a Dedicated Job First

- isolates the heaviest dependency
- avoids destabilizing the existing fast matrix
- makes failures easier to diagnose during bring-up

### Local and Repo Wiring Changes

The plan should include updates to:

- `pyproject.toml` for a new `mq` extra
- `README.md` installation examples
- `docker-compose.yaml`
- `docs/includes/docker-compose.yaml`
- `.github/workflows/pr_tests.yaml`
- pytest markers for `mq`

## AsyncAPI Strategy

There does not appear to be a well-established IBM MQ AsyncAPI binding to mirror the existing RabbitMQ or Kafka-specific bindings.

### Recommended MVP Behavior

- emit normal AsyncAPI channels, messages, and operations
- do not invent an IBM MQ binding in the first pass
- keep any MQ-specific details out of the schema unless a clean vendor-extension story is agreed later

This keeps schema generation honest and avoids publishing misleading protocol metadata.

## Observability Strategy

Add standard FastStream observability integration for the IBM MQ provider.

### Prometheus Provider

Add `faststream/mq/prometheus/provider.py`.

Suggested metrics attributes:

- destination queue name
- message size
- message count

### OpenTelemetry Provider

Add `faststream/mq/opentelemetry/provider.py`.

Suggested span attributes:

- messaging system = `ibm_mq` or `ibmmq`
- destination queue name
- message ID
- correlation ID
- payload size
- reply queue name where applicable

### Propagation Note

If `ibmmq` or IBM MQ examples provide their own optional tracing helpers, FastStream middleware should remain the canonical propagation mechanism for this provider.

## Documentation Plan

Add a minimal but complete first documentation slice.

### Root-Level Docs and Metadata

- update `README.md`
- add installation snippet for `faststream[mq]`
- mention IBM MQ in the supported brokers list when the feature is ready

### Provider Docs

Add docs pages parallel to the other providers:

- provider index page
- publishing page
- message page
- request/reply page
- acknowledgement page
- security/configuration page

### Examples

Add examples for:

- basic consume/publish
- request/reply
- manual ack/nack
- FastAPI integration

## File-by-File Implementation Checklist

### Phase 1: Scaffolding and Packaging

- add `faststream/mq/` package skeleton
- add top-level exports
- add `mq` optional dependency in `pyproject.toml`
- add import error/help text if needed in `faststream/exceptions.py`
- add pytest marker for `mq`
- add placeholder tests and mypy coverage file

### Phase 2: Core Runtime Plumbing

- implement connection config and security parsing
- implement low-level MQ client wrapper/helpers
- implement producer connection management
- implement subscriber connection management
- implement parser and message model

### Phase 3: Publish and Consume

- implement publisher use case
- implement subscriber use case
- implement syncpoint-aware receive flow
- implement `ack()` and `nack()`
- document and implement MVP `reject()` behavior

### Phase 4: Request/Reply

- implement dynamic reply queue creation
- implement request publish flow
- implement reply receive flow
- implement subscriber auto-reply behavior
- add request/reply tests

### Phase 5: Router, FastAPI, and Test Broker

- implement router/registrator support
- implement FastAPI router integration
- implement `TestMQBroker`
- make generic broker tests pass under fake mode

### Phase 6: Observability and AsyncAPI

- implement Prometheus settings provider
- implement OpenTelemetry settings provider
- add minimal AsyncAPI support
- add schema tests

### Phase 7: Docs, Examples, and CI

- update `README.md`
- add docs pages and examples
- add IBM MQ service/dev configuration
- add dedicated CI job
- stabilize connected tests

## Open Decisions to Resolve Early

These decisions should be made before deep implementation begins:

1. package name: `faststream.mq` vs a more explicit IBM-specific name
2. exact constructor shape for auth and connection options
3. final MVP behavior of `reject()`
4. whether to store FastStream `correlation_id` primarily in properties, MQMD, or both
5. whether local/CI password handling should use mounted secrets from day one or temporary env-based developer defaults

## Recommended Order of Work

1. finalize the public package name and constructor shape
2. scaffold the package and dependency wiring
3. build the low-level blocking-client bridge
4. implement queue publish/consume with `ack()` and `nack()`
5. add fake broker support
6. implement request/reply
7. add connected tests with PCF-managed temporary queues
8. add telemetry and AsyncAPI
9. finish docs, examples, and CI hardening

## Acceptance Checklist for the First Mergeable PR Set

- `faststream[mq]` installs cleanly in the intended environment
- fake broker tests pass
- connected MQ tests pass in a dedicated job
- `publish`, `subscriber`, and `request` flows work end-to-end
- `ack()` and `nack()` are documented and tested
- basic docs/examples exist
- maintainers can run the provider locally with the documented IBM MQ container setup

## Notes for Future Iterations

After the queue MVP is merged and stable, the next likely extensions are:

- IBM MQ topic/pubsub support
- richer security configuration coverage
- configurable reject/backout policies
- dead-letter/backout-queue helpers
- deeper AsyncAPI customization if the ecosystem standardizes around IBM MQ metadata
- performance tuning and concurrency expansion
