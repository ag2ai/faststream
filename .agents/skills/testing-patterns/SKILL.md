---
name: testing-patterns
description: Choosing which tests to run after a change, or writing tests under tests/ — base testcases, markers, in-memory vs connected brokers.
---

# FastStream Testing Patterns

## Run only the test files the change touches

Name the specific test files:

```bash
uv run pytest tests/brokers/kafka/test_misconfigure.py tests/asyncapi/kafka/v3_0_0/test_address.py -m "not connected"
```

The set is the files that could see the edit, and it stops there. A directory
or a bare `tests/` is CI's shape: CI runs everything anyway, so a local run
exists to answer whether *this* edit works, and every test beyond that is time
spent not finding out.

To find the files: grep the symbol you changed across `tests/`, and follow the
naming — a change to `faststream/<broker>/<endpoint>/<thing>.py` is usually
covered by `tests/brokers/<broker>/test_<thing>.py` and
`tests/asyncapi/<broker>/v*/test_<thing>.py`.

A shared testcase in `tests/brokers/base/` or `tests/asyncapi/base/` is
inherited by every broker, so editing one does widen the set — but widen it by
naming the inheriting files, not by running their directories.

**Reach for `connected` only when the change reaches the wire** — subscribing,
publishing, acks, bindings, reconnects. A change to a specification, a config,
or a declaration-time check is decided in memory, in seconds. Broker-backed
runs take minutes and share state between runs.

When a `connected` run does fail, **re-run the failures alone before believing
them**. The brokers are shared and accumulate topics, queues and consumer
groups; a failure that passes in isolation is the container, not the code. To
tell them apart, run the same set against the committed tree (`git stash`) and
compare counts.

## Commands

Run pytest directly or via just — **never through the rtk proxy**.

Direct pytest needs no container, which is why named files run there. The
`just test*` recipes run whole suites inside the dev container
(`docker compose exec faststream`, so `just up` first):

- `just test [path]` — fast selection: `-m "not slow and not connected"`, parallel `-n auto`. Takes a path, so it can be pointed at named files.
- `just test-kafka` / `test-rabbit` / `test-nats` / `test-redis` / `test-redis-cluster` / `test-confluent` — a whole broker, excluding `connected` and `slow`; the `-all` variants add the slow and connected ones (that broker must be up).
- `just test-all` — everything (`-m "all"`).

Heads-up: the pyproject default addopts exclude only `slow` (`-m 'not slow'`) — bare pytest WILL collect `connected` tests, so pass `-m "not slow and not connected"` explicitly when no broker is running.

Global pytest timeout is 30s per test; the suite runs parallel — keep tests independent and use the `queue` fixture for unique names.

## Markers — strict

`--strict-markers` is enabled; the allowed set is defined in `pyproject.toml` (`kafka`, `confluent`, `rabbit`, `nats`, `redis`, `redis_cluster`, `mqtt`, `slow`, `connected`, `all`, `benchmark`).

- Broker-specific test → its broker mark: `@pytest.mark.kafka()`.
- Talks to a real broker over the network → add `@pytest.mark.connected()` (excluded by `just test`; bare pytest excludes only `slow` by default).
- Slow test → `@pytest.mark.slow()` (also excluded by default).
- Async test → `@pytest.mark.asyncio()`.

## Shared base testcases

Cross-broker behavior is specified ONCE in `tests/brokers/base/` (`basic.py`, `consume.py`, `publish.py`, `router.py`, `codec.py`, `middlewares.py`, `parser.py`, `requests.py`, `connection.py`, `fastapi.py`, `testclient.py`, ...) and inherited by every broker.

Each broker defines its config in `tests/brokers/<broker>/basic.py`:

```python
class KafkaTestcaseConfig(BaseTestcaseConfig):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> KafkaBroker:
        return KafkaBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> KafkaRouter:
        return KafkaRouter(**kwargs)


class KafkaMemoryTestcaseConfig(KafkaTestcaseConfig):
    def patch_broker(self, *brokers: KafkaBroker, **kwargs: Any) -> TestKafkaBroker:
        return TestKafkaBroker(*brokers, **kwargs)
```

Test classes multiply-inherit config + behavior suite:

```python
@pytest.mark.kafka()
class TestKafkaCodec(KafkaMemoryTestcaseConfig, CodecTestcase): ...


@pytest.mark.connected()
@pytest.mark.kafka()
class TestConsume(KafkaTestcaseConfig, BrokerRealConsumeTestcase): ...
```

**Rule:** new cross-broker behavior goes into a base class in `tests/brokers/base/` so every broker inherits the test. Broker-specific behavior is tested directly in `tests/brokers/<broker>/`.

**Hook surface:** a member of a base testcase earns its place by being overridden in `tests/brokers/<broker>/` — `separator`, `declare_subscriber` and `publish` in `base/address.py` are hooks because MQTT, Kafka and RabbitMQ respell them, and each docstring names who does. What no broker overrides goes inline in the test body, beside the assertion it feeds, with a comment carrying the value it compiles to:

```python
# subscribe to "queue.{level}"
subscriber = self.declare_subscriber(
    broker,
    f"{queue}{self.separator}{{level}}",
    queue,
)
```

## Regression tests

A test defending a fixed bug names the issue by **full URL**, so the case it pins is one click away:

```python
@pytest.mark.xfail(reason="https://github.com/ag2ai/faststream/issues/2513")
async def test_publisher_without_destination(self) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2513."""
```

The URL goes on the docstring's own first line, with the explanation of the behavior below it. `xfail`/`skip` reasons take the same URL. Comments inside the test body follow the **code-architecture** rules — two lines, over the line they explain.

A test written after the fix earns its place by going **red** on the old code — revert the fix, run, restore:

```bash
git show <fix-commit> -- faststream/ | git apply -R -
uv run pytest tests/... -m "not slow and not connected"
git checkout -- faststream/
```

The same run grades the tests already there, and it is how a suite shrinks. Two tests red for one reason are one test: keep the one whose declaration carries more (an escaped brace *beside* a Path parameter over an escaped brace alone), delete the other, and check what a broker already gets from `test_router.py` or `test_path.py` before keeping a third.

## In-memory vs real broker

- Default to the in-memory `TestBroker` (`faststream/<broker>/testing.py`) via a `*MemoryTestcaseConfig` — fast, runs everywhere, no `connected` mark.
- Use a real broker (plain `*TestcaseConfig` + `@pytest.mark.connected()`) when the behavior depends on actual broker semantics (acks, consumer groups, reconnects). Connection settings come from the `Settings` dataclass in `tests/brokers/<broker>/conftest.py`.

## Fixtures & utilities

- Global fixtures (`tests/conftest.py`): `queue` (unique uuid string), `event` (`asyncio.Event`), `mock` / `async_mock` (function-scoped, reset via teardown), `context`, `runner` (CLI).
- `tests/marks.py`: conditional skips — `skip_windows`, `skip_macos`, `pydantic_v1`/`pydantic_v2`, `require_aiokafka`, `require_confluent`, `require_aiopika`, `require_redis`, `require_nats`, `require_mqtt`.
- `tests/tools.py`: `spy_decorator` — wraps a real method with a mock spy (call assertions via `.mock`) while preserving behavior.
- `tests/mocks.py`: `mock_pydantic_settings_env` for env-driven settings tests.
- `dirty-equals` and `freezegun` are available as test deps.

**Never import from a `conftest.py`.** pytest loads conftest modules specially (their fixtures are injected into the collected files), so importing from one — `from .conftest import Settings` or `from tests.brokers.redis.conftest import ...` — can produce a duplicated/mismatched module and confusing collection errors. When conftest and a test file need the same object, declare it in a plain helper module next to them (e.g. `tests/brokers/redis/settings.py`, `basic.py`) and import it from both.

## Related skills

- **dev-workflow** — docker broker management and the full just recipe matrix.
- **code-architecture** — where the code under test lives and how it's shaped.
- **documentation-writing** — docs snippets get tests under `tests/docs/`.
