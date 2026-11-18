---
name: code-architecture
description: Use when writing or reviewing library code under faststream/ for broker architecture, typing, configuration, or public API compatibility.
---

# FastStream Code Architecture

## Public vs internal split

- `faststream/_internal/` holds shared machinery: `broker/` (abstract `BrokerUsecase`, registrator, router), `endpoint/`, `di/` (fast-depends integration), `context/`, `configs/`, `logger/`, `testing/`, `cli/`, `fastapi/`, `utils/`.
- Broker packages (`faststream/kafka/`, `rabbit/`, `nats/`, `redis/`, `confluent/`, `mqtt/`) are thin public layers over `_internal`.
- Cross-broker public packages: `faststream/middlewares/`, `params/`, `response/`, `specification/`, `message/`, `asgi/`, `opentelemetry/`, `prometheus/`.

**Rule:** implement shared behavior in `_internal/`, expose it through broker packages. User-facing code (docs, examples, error messages) must never import from `faststream._internal`.

- **A field only one broker needs lives in that broker's config**, not in the shared one.
- **A neighbour is read through its public surface.** `_foo` of another module is outside its contract, even inside `_internal/`.

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

A broker package is closed over its driver: `faststream.<broker>` never imports another broker package, nothing outside it imports it, and its driver (`aiokafka`, `confluent_kafka`, `aio_pika`, `nats`, `redis`, `zmqtt`) is imported nowhere else. `just import-linter` checks all three; the contracts live in `[tool.importlinter]` in `pyproject.toml`.

## Shape of a public object

- **An extensible object, not a magic dict.** A structure a user passes or receives is a class with named fields (`Response`, `PublishMessage`), not a free-form dict; a format is a class, not a boolean flag.
- **`broker.subscriber()` is a facade with no logic.** Assembly belongs to the factory; the DTO validates itself and exposes derived values through `@property`.
- **Handler metadata lives on a class, not as an attribute stapled to the function.** Constructor options are keyword-only.
- **A message method reads the fields the parser lifted onto it.** Everything the framework needs later comes off the message object; a method that reaches into the driver's payload is re-parsing it. `raw_message` itself is for handing back to the driver (`ack`, `commit`), for the broker-specific attributes of telemetry, and for the user.

## Feature mirroring

All brokers expose the same surface: `publish()`, `request()`, `ping()`, `start()`, `stop()`, routers, publishers, message/response types. When adding a feature:

1. Find the closest analogue in another broker (kafka is usually the most complete) and follow its shape and naming.
2. Keep the public API identical across brokers unless the feature is inherently broker-specific.
3. Broker-specific features stay in the broker package — don't leak them into `_internal/`.
4. **Kafka has two backends.** A fix in `faststream/kafka/` (aiokafka) is mirrored into `faststream/confluent/` in the same PR, and vice versa.
5. Logic that does not depend on the broker lives in the shared class, and values shared by all brokers go through one common type (e.g. `Address`) rather than a per-broker string.
6. **An invariant is established once, at the entry point**, and every reader takes it from there: the default of an outgoing message is applied in `producer._publish` alone. Two names for one value is a bug, not a convenience.

## Option surface

- **Scope decides the level.** A **scoped** option — one a user could want different for different parts of one application — exists at every level: `broker` → `router` → `subscriber`/`publisher` → FastAPI router, innermost wins, each level tested. A **global** setting, one that describes the application as a single object, lives at the root alone.
- One knob, not two. Prefer a single parameter over a `bool` + `str` pair; `None` disables it.
- A default that depends on a neighbouring parameter is derived through the `EMPTY` sentinel, not by guessing inside the body.
- Behaviour that differs by **broker/server version** lives in the versioned implementation, not behind `if self._version` scattered through the broker. (Python and Pydantic differences go through `_compat` — see Typing.)

## Typing

- mypy runs with `strict = true` (see `[tool.mypy]` in `pyproject.toml`): every function fully annotated, no implicit `Optional`, decorators typed. Checked paths: `faststream/`, all of `tests/` and `docs/docs_src/`.
- Generics are used for broker abstractions: `BrokerUsecase[MsgType, ConnectionType, BrokerConfigType]` (see `faststream/_internal/broker/broker.py`), `BaseMiddleware[PublishCommandType, AnyMsg]`.
- Import `Callable`, `Awaitable`, `Sequence`, `Mapping` from `collections.abc`; newer typing features (`Self`, `ParamSpec`, `TypedDict`, ...) from `typing_extensions`.
- Connection kwargs use `TypedDict` (e.g. `KafkaInitKwargs` in `faststream/kafka/broker/broker.py`).
- Pydantic v1/v2 and Python-version differences go through `faststream/_internal/_compat.py` — never inline version checks elsewhere.

## Configs

Config classes are `@dataclass(kw_only=True)` inheriting `BrokerConfig` (base in `faststream/_internal/configs/`). Example: `faststream/kafka/configs/broker.py`.

## Public API

- Every `__init__.py` declares `__all__` explicitly, and every name in it resolves at runtime — a name imported only under `if TYPE_CHECKING:` passes mypy and fails in production. `tests/test_public_exports.py` imports each public module and reads every name it exports.
- Optional dependencies are guarded with try/except raising an `ImportError` that tells the user which extra to install — see `faststream/kafka/__init__.py`.
- **A user imports the driver for its exception types.** FastStream re-exports none of them.
- A distinct connection mode (Cluster, Sentinel) is its own broker class, not a flag on the existing one.
- An endpoint returns the result itself. No envelope, no wrapper object around it.

## Invariants review checks by hand

**Compatibility**

- Identity semantics and any behaviour pinned by an existing test are a contract. They change on a bug report, not on the way past.
- A new positional parameter goes **after** the existing ones. Anything that cannot preserve the old call sites waits for a major release.
- **A public attribute is not removed by a refactor**, and a new option does not change an existing default.
- New code does not introduce a deprecated API, even when the surrounding module still uses one.
- **`DeprecationWarning` goes where the user makes the choice** — at every intake point and in every `@overload` — and fires on an explicit choice, never on `EMPTY`. Warning a user about a default they never selected is noise.
- A third-party incompatibility is solved by an adapter on the user's side, not by bending the framework; a fix stays inside the reported problem; when part of a contribution is wrong, that part is cut and the rest is merged.
- A pattern the framework cannot support is forbidden loudly, with a link to the docs — not patched around so it half-works.

**Errors and lifecycle**

- An infrastructure failure gets its own exception type; it is not folded into a generic one.
- If recovery requires a configuration change, stop the consumer and report the invalid setting. If the failure can clear on its own and the consumer supports retries, wait between attempts.
- One failure, one exception — the same condition raises the same type on every consumption path (`get_one`, iterator, subscriber).
- A configuration conflict warns at **registration** time, with `stacklevel` pointing at the user's line, and only when the values actually differ.
- When filtering driver options, report any explicit setting the driver cannot honour. Silently dropping a TLS setting can change the security of the connection.
- **A class-level container with no eviction is a leak.** Anything keyed per instance and never cleaned belongs to the instance.
- Static data is separated from dynamic once, at initialisation, not re-computed on the hot path.
- `connect()` → `setup_logger()` ordering is a contract. Shutdown order is `running = False` → wait for in-flight → `super().stop()` under the lock, and an object is removed from its registry only after `stop()` completes.
- A string built for a log line is not reused as an address, key or identifier. Display and identity are separate values.

## AsyncAPI schema

- The schema follows the specification, not what is convenient to generate.
- An explicit `title` is used as given — never mangled.
- A subscriber on N addresses produces N channels.
- Only the application's own routes end up in the schema; routes mounted from elsewhere do not.

## Style

- ruff uses `select = ["ALL"]` with curated ignores in `ruff.toml` — don't assume a rule is disabled; run `just linter` to check.
- `just linter` and `just mypy` must pass before a PR.

### Module layout

- **Public first.** A module opens with the class or function it exists for, then the rest of the public surface by significance, then `_`-prefixed helpers. Quote an annotation (`syntax: "AddressSyntax"`) when the headline class refers to something defined below it. A reorder gets its own commit, so it cannot hide a behaviour change.
- **A helper that never reads `self` is a module-level function** (`_name` in the library). `PLR6301` enforces it in `faststream/` only: an override takes `@override`, a hook users subclass keeps `self` under `# noqa: PLR6301`. Test classes, class-scoped fixtures and testcase hooks stay methods.
- **A heavy import stays lazy.** The AsyncAPI generators (`specification/asyncapi/v2_6_0`, `v3_0_0`) build Pydantic models on import. Loading them inside the schema-generation path keeps that work out of `import faststream`. Before moving a local import to the top, run `python -X importtime -c "import faststream"`; a module that must stay lazy is listed in `banned-module-level-imports` in `ruff.toml`, so the lint holds it there.

### Docstrings

A docstring is a one-line summary, or a Google `Args:` section when parameters need explaining — measure the neighbours: none of `faststream/rabbit/*.py` has a module docstring, and their functions carry one-liners. The reasoning goes in a comment.

### Comments

A comment carries the **why** — the constraint, the surprise, the reason this line is not the obvious one. What the code does is already on the screen.

- **Two lines, maximum.** A caveat needing more than that is usually a docstring, an ADR, or a sign the code should be clearer.
- **Directly above the line it explains**, so the reader meets the explanation at the moment the code surprises them. A caveat about one assertion belongs over that assertion, not in the docstring header several screens up.
- When a bug fix needs a comment, explain the constraint that makes the fix necessary. Include the issue number so readers can find the original report.

## Related skills

- **testing-patterns** — every source change needs tests following the base-testcase model.
- **dev-workflow** — full command reference (lint, mypy, docker brokers).
- **documentation-writing** — user-facing features need docs with tested snippets.
