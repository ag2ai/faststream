---
name: code-architecture
description: Use when writing or modifying FastStream library source code under faststream/ — package layout, broker package anatomy, typing rules, configs, and public API conventions.
---

# FastStream Code Architecture

## Public vs internal split

- `faststream/_internal/` holds shared machinery: `broker/` (abstract `BrokerUsecase`, registrator, router), `endpoint/`, `di/` (fast-depends integration), `context/`, `configs/`, `logger/`, `testing/`, `cli/`, `fastapi/`, `utils/`.
- Broker packages (`faststream/kafka/`, `rabbit/`, `nats/`, `redis/`, `confluent/`, `mqtt/`) are thin public layers over `_internal`.
- Cross-broker public packages: `faststream/middlewares/`, `params/`, `response/`, `specification/`, `message/`, `asgi/`, `opentelemetry/`, `prometheus/`.

**Rule:** implement shared behavior in `_internal/`, expose it through broker packages. User-facing code (docs, examples, error messages) must never import from `faststream._internal`.

## Broker package anatomy

Every broker package mirrors the same layout. Canonical reference: `faststream/kafka/`.

```
faststream/<broker>/
├── __init__.py        # public exports with explicit __all__
├── annotations.py     # broker-specific Annotated type aliases
├── broker/            # broker.py (BrokerUsecase subclass), router.py, registrator.py, logging.py
├── configs/           # @dataclass(kw_only=True) configs inheriting BrokerConfig
├── message.py         # StreamMessage subclass
├── parser.py          # message parser
├── publisher/         # publisher endpoint + producer.py
├── subscriber/        # subscriber endpoint (usecase.py; nats/redis split into usecases/)
├── response.py        # PublishCommand subclasses
├── security.py        # auth/security helpers
├── testing.py         # in-memory TestBroker
└── exceptions.py      # broker-specific exceptions
```

Brokers also carry optional integration subpackages where supported — kafka has `fastapi/`, `helpers/`, `opentelemetry/`, `prometheus/`, and `schemas/` — follow kafka's structure when adding these to another broker.

## Feature mirroring

All brokers expose the same surface: `publish()`, `request()`, `ping()`, `start()`, `stop()`, routers, publishers, message/response types. When adding a feature:

1. Find the closest analogue in another broker (kafka is usually the most complete) and follow its shape and naming.
2. Keep the public API identical across brokers unless the feature is inherently broker-specific.
3. Broker-specific features stay in the broker package — don't leak them into `_internal/`.

## Typing

- mypy runs with `strict = true` (see `[tool.mypy]` in `pyproject.toml`): every function fully annotated, no implicit `Optional`, decorators typed. Checked paths: `faststream/` and `tests/mypy/`.
- Generics are used for broker abstractions: `BrokerUsecase[MsgType, ConnectionType, BrokerConfigType]` (see `faststream/_internal/broker/broker.py`), `BaseMiddleware[PublishCommandType, AnyMsg]`.
- Import `Callable`, `Awaitable`, `Sequence`, `Mapping` from `collections.abc`; newer typing features (`Self`, `ParamSpec`, `TypedDict`, ...) from `typing_extensions`.
- Connection kwargs use `TypedDict` (e.g. `KafkaInitKwargs` in `faststream/kafka/broker/broker.py`).
- Pydantic v1/v2 and Python-version differences go through `faststream/_internal/_compat.py` — never inline version checks elsewhere.

## Configs

Config classes are `@dataclass(kw_only=True)` inheriting `BrokerConfig` (base in `faststream/_internal/configs/`). Example: `faststream/kafka/configs/broker.py`.

## Public API

- Every `__init__.py` declares `__all__` explicitly.
- Optional dependencies are guarded with try/except raising an `ImportError` that tells the user which extra to install — see `faststream/kafka/__init__.py`.

## Style

- ruff uses `select = ["ALL"]` with curated ignores in `ruff.toml` — don't assume a rule is disabled; run `just linter` to check.
- Line length 90, double quotes, Google-style docstrings.
- `just mypy` must pass before a PR.

## Related skills

- **testing-patterns** — every source change needs tests following the base-testcase model.
- **dev-workflow** — full command reference (lint, mypy, docker brokers).
- **documentation-writing** — user-facing features need docs with tested snippets.
