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

Three directions the boundary is crossed in review (#2038, #2644, #2290):

- **Core does not import a broker.** Shared code never reaches for a concrete broker package.
- **Broker specifics do not leak into the shared config.** If only one broker needs the field, it belongs to that broker's config.
- **A neighbour's private is not read.** `_foo` of another module is not part of its contract, even inside `_internal/`.

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

## Shape of a public object

- **An extensible object, not a magic dict.** A structure a user passes or receives is a class with named fields (`Response`, `PublishMessage`), not a free-form dict; a format is a class, not a boolean flag (#2586, #2287).
- **`broker.subscriber()` is a facade with no logic.** Assembly belongs to the factory; the DTO validates itself and exposes derived values through `@property` (#2038).
- **Handler metadata lives on a class, not as an attribute stapled to the function.** Constructor options are keyword-only (#2142).
- **The default of an outgoing message is applied in one place** — `producer._publish` — not re-derived by every caller (#2226).

## Feature mirroring

All brokers expose the same surface: `publish()`, `request()`, `ping()`, `start()`, `stop()`, routers, publishers, message/response types. When adding a feature:

1. Find the closest analogue in another broker (kafka is usually the most complete) and follow its shape and naming.
2. Keep the public API identical across brokers unless the feature is inherently broker-specific.
3. Broker-specific features stay in the broker package — don't leak them into `_internal/`.
4. **Kafka has two backends.** A fix in `faststream/kafka/` (aiokafka) is mirrored into `faststream/confluent/` in the same PR, and vice versa (#2932).
5. Logic that does not depend on the broker lives in the shared class, and values shared by all brokers go through one common type (e.g. `Address`) rather than a per-broker string (#3072, #3042).
6. An invariant is established once, at the entry point — not re-derived by every reader. Two names for one value is a bug, not a convenience (#3072).

## Option surface

- A user-facing option exists at **every** level it can reasonably be set: `broker` → `router` → `subscriber`/`publisher` → FastAPI router. The innermost level wins; each level gets its own test (#2871, #2827, #3026).
- The inverse also holds: an option that belongs to one object stays on that object and is not duplicated upward. A route-scoped setting does not become an application-level setting (#2777).
- One knob, not two. Prefer a single parameter over a `bool` + `str` pair; `None` disables it (#2894).
- A default that depends on a neighbouring parameter is derived through the `EMPTY` sentinel, not by guessing inside the body (#2894).
- Behaviour that differs by **broker/server version** lives in the versioned implementation, not behind `if self._version` scattered through the broker (#2819). (Python and Pydantic differences go through `_compat` — see Typing.)

## Typing

- mypy runs with `strict = true` (see `[tool.mypy]` in `pyproject.toml`): every function fully annotated, no implicit `Optional`, decorators typed. Checked paths: `faststream/`, all of `tests/` and `docs/docs_src/`.
- Generics are used for broker abstractions: `BrokerUsecase[MsgType, ConnectionType, BrokerConfigType]` (see `faststream/_internal/broker/broker.py`), `BaseMiddleware[PublishCommandType, AnyMsg]`.
- Import `Callable`, `Awaitable`, `Sequence`, `Mapping` from `collections.abc`; newer typing features (`Self`, `ParamSpec`, `TypedDict`, ...) from `typing_extensions`.
- Connection kwargs use `TypedDict` (e.g. `KafkaInitKwargs` in `faststream/kafka/broker/broker.py`).
- Pydantic v1/v2 and Python-version differences go through `faststream/_internal/_compat.py` — never inline version checks elsewhere.

## Configs

Config classes are `@dataclass(kw_only=True)` inheriting `BrokerConfig` (base in `faststream/_internal/configs/`). Example: `faststream/kafka/configs/broker.py`.

## Public API

- Every `__init__.py` declares `__all__` explicitly.
- **Every name in `__all__` must resolve at runtime.** A name imported only under `if TYPE_CHECKING:` passes mypy and fails in production: `docs/create_api_docs.py` walks `__all__` and calls `getattr(module, name)` (#2841 → #2898).
- Optional dependencies are guarded with try/except raising an `ImportError` that tells the user which extra to install — see `faststream/kafka/__init__.py`.
- Driver exceptions are **not** re-exported through FastStream. Driver types are used by importing the driver (#2911, #2819).
- A distinct connection mode (Cluster, Sentinel) is its own broker class, not a flag on the existing one (#2895).
- An endpoint returns the result itself. No envelope, no wrapper object around it (#2777).

## Invariants review checks by hand

Nothing below is caught by ruff, mypy or the test suite. Every entry is backed by a bug that reached `main`
or a rewrite that landed on top of a merged contribution.

**Compatibility**

- Identity semantics and any behaviour pinned by an existing test are a contract. They change on a bug report, not on the way past (#2796).
- A new positional parameter goes **after** the existing ones. Anything that cannot preserve the old call sites waits for a major release (#2894, #2828, #2777).
- **A public attribute is not removed by a refactor**, and a new option does not change an existing default (#2038, #2572).
- New code does not introduce a deprecated API, even when the surrounding module still uses one (#2819).
- **`DeprecationWarning` goes where the user makes the choice** — at every intake point and in every `@overload` — and fires on an explicit choice, never on `EMPTY`. Warning a user about a default they never selected is noise (#2236, #2287, #2819).
- A third-party incompatibility is solved by an adapter on the user's side, not by bending the framework; a fix stays inside the reported problem; when part of a contribution is wrong, that part is cut and the rest is merged (#2828, #2373, #2127, #2142).
- A pattern the framework cannot support is forbidden loudly, with a link to the docs — not patched around so it half-works (#2828).

**Errors and lifecycle**

- An infrastructure failure gets its own exception type; it is not folded into a generic one (#2855).
- A configuration error is terminal: stop the consumer, do not retry. Retrying a permanent failure hides it and burns the broker (#3049 → #3115).
- One failure, one exception — the same condition raises the same type on every consumption path (`get_one`, iterator, subscriber) (#3049).
- A configuration conflict warns at **registration** time, with `stacklevel` pointing at the user's line, and only when the values actually differ (#3026, #2849).
- A parameter filter never silently drops user intent. If an option cannot be honoured, say so — do not pass a subset on (#2935: TLS settings were dropped silently).
- An unrecoverable error waits with a pause; it never spins in an idle loop (#2319).
- `ping(timeout)` honours the timeout it was given (#2212).
- **A class-level container with no eviction is a leak.** Anything keyed per instance and never cleaned belongs to the instance (#2661).
- Static data is separated from dynamic once, at initialisation, not re-computed on the hot path (#2555).
- The access-log cost (about 30 of 57 µs per message) is stdout I/O, not record building: `Logger.log` already short-circuits on level, so an `isEnabledFor` guard buys 0.02 µs. Dropping those records changes what `log_level` means — a public-API decision, not an optimisation (#3130).
- `connect()` → `setup_logger()` ordering is a contract. Shutdown order is `running = False` → wait for in-flight → `super().stop()` under the lock, and an object is removed from its registry only after `stop()` completes (#2531, #2859, #3108).
- A string built for a log line is not reused as an address, key or identifier. Display and identity are separate values (#3041 → #3070).

## AsyncAPI schema

- The schema follows the specification, not what is convenient to generate (#2142).
- An explicit `title` is used as given — never mangled (#2638).
- A subscriber on N addresses produces N channels (#3070).
- Only the application's own routes end up in the schema; routes mounted from elsewhere do not.

## Style

- ruff uses `select = ["ALL"]` with curated ignores in `ruff.toml` — don't assume a rule is disabled; run `just linter` to check.
- Line length 90, double quotes, Google-style docstrings.
- `just mypy` must pass before a PR.

### Module layout

- **Public first.** A module opens with the class or function it exists for, then the rest of the public surface by significance, then `_`-prefixed helpers. Quote an annotation (`syntax: "AddressSyntax"`) when the headline class refers to something defined below it. A reorder gets its own commit, so it cannot hide a behaviour change (#3072).
- **A helper that never reads `self` is a module-level function** (`_name` in the library). `PLR6301` enforces it in `faststream/` only: an override takes `@override`, a hook users subclass keeps `self` under `# noqa: PLR6301`. Test classes, class-scoped fixtures and testcase hooks stay methods (#3049, #3158).
- **A heavy import stays lazy.** The AsyncAPI generators (`specification/asyncapi/v2_6_0`, `v3_0_0`) build pydantic models on import: hoisting them cost `import faststream` +28 ms and 71 modules. Before moving a local import to the top, run `python -X importtime -c "import faststream"`; a module that must stay lazy is listed in `banned-module-level-imports` in `ruff.toml`, so the lint holds it there (#3158).

### Docstrings

A docstring is a one-line summary, or a Google `Args:` section when parameters need explaining — measure the neighbours: none of `faststream/rabbit/*.py` has a module docstring, and their functions carry one-liners. The reasoning goes in a comment (#3109).

### Comments

A comment carries the **why** — the constraint, the surprise, the reason this line is not the obvious one. What the code does is already on the screen.

- **Two lines, maximum.** A caveat needing more than that is usually a docstring, an ADR, or a sign the code should be clearer.
- **Directly above the line it explains**, so the reader meets the explanation at the moment the code surprises them. A caveat about one assertion belongs over that assertion, not in the docstring header several screens up.
- **Name the issue** when the code exists because of a reported bug: `# ... which no broker can express (see issue #3056).` The next reader gets the whole investigation for free.

## Related skills

- **testing-patterns** — every source change needs tests following the base-testcase model.
- **dev-workflow** — full command reference (lint, mypy, docker brokers).
- **documentation-writing** — user-facing features need docs with tested snippets.
