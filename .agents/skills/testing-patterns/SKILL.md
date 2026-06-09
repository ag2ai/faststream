---
name: testing-patterns
description: Use when writing or modifying tests under tests/ — base testcase inheritance, pytest markers, fixtures, in-memory vs real-broker testing, and how to run the suite.
---

# FastStream Testing Patterns

## Running tests

Run pytest directly or via just — **never through the rtk proxy**.

All `just test*` recipes run inside the dev container (`docker compose exec faststream`) — start it with `just up` first.

- `just test [path]` — fast suite: `-m "not slow and not connected"`, parallel `-n auto`.
- `just test-kafka` / `test-rabbit` / `test-nats` / `test-redis` / `test-redis-cluster` / `test-confluent` — per-broker subset excluding `connected` and `slow`; the `-all` variants run every broker-marked test including slow/connected ones (that broker must be up).
- `just test-all` — the full suite (`-m "all"`).
- Direct, no container needed: `uv run pytest tests/... -m "not slow and not connected"`.

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

## In-memory vs real broker

- Default to the in-memory `TestBroker` (`faststream/<broker>/testing.py`) via a `*MemoryTestcaseConfig` — fast, runs everywhere, no `connected` mark.
- Use a real broker (plain `*TestcaseConfig` + `@pytest.mark.connected()`) when the behavior depends on actual broker semantics (acks, consumer groups, reconnects). Connection settings come from the `Settings` dataclass in `tests/brokers/<broker>/conftest.py`.

## Fixtures & utilities

- Global fixtures (`tests/conftest.py`): `queue` (unique uuid string), `event` (`asyncio.Event`), `mock` / `async_mock` (function-scoped, reset via teardown), `context`, `runner` (CLI).
- `tests/marks.py`: conditional skips — `skip_windows`, `skip_macos`, `pydantic_v1`/`pydantic_v2`, `require_aiokafka`, `require_confluent`, `require_aiopika`, `require_redis`, `require_nats`, `require_mqtt`.
- `tests/tools.py`: `spy_decorator` — wraps a real method with a mock spy (call assertions via `.mock`) while preserving behavior.
- `tests/mocks.py`: `mock_pydantic_settings_env` for env-driven settings tests.
- `dirty-equals` and `freezegun` are available as test deps.

## Related skills

- **dev-workflow** — docker broker management and the full just recipe matrix.
- **code-architecture** — where the code under test lives and how it's shaped.
- **documentation-writing** — docs snippets get tests under `tests/docs/`.
