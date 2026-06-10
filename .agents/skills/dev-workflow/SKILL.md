---
name: dev-workflow
description: Use when setting up the FastStream dev environment, running tests/linters/static analysis, managing docker brokers, or preparing a PR.
---

# FastStream Dev Workflow

## Toolchain

`uv` for Python deps + `just` as the task runner + docker compose for brokers. Never use bare `pip`. Run `just` with no args to list all recipes.

- `just init [python-version]` — build the dev environment (default Python 3.10).

## Docker brokers

- `just up` / `just stop` / `just down` — start / stop / remove all containers (brokers + the `faststream` dev container the test recipes exec into).
- Per broker: `just kafka-up`, `rabbit-up`, `nats-up`, `redis-up`, `redis-cluster-up` (+ matching `-stop` and `-logs` recipes). MQTT has no local just recipes — it is tested in CI only.

## Test matrix

All `just test*` recipes execute inside the dev container (`docker compose exec faststream`) — run `just up` first.

| Command | Selection |
|---|---|
| `just test [path]` | fast: `not slow and not connected`, parallel `-n auto` |
| `just test-all [path]` | full suite (`-m "all"`) — all brokers must be up |
| `just test-<broker> [path]` | broker-marked tests excluding `connected` and `slow` (kafka, confluent, rabbit, redis, redis-cluster, nats) |
| `just test-<broker>-all [path]` | every broker-marked test incl. slow/connected — needs that broker up |
| `just test-coverage [path]` / `test-coverage-all` | with coverage |

Extra pytest args pass through: `just test tests/brokers/kafka -vv`. Run pytest directly when needed (`uv run pytest ...` — no container required, but bare pytest excludes only `slow` by default) — never via the rtk proxy.

## Lint & static analysis (run before any PR, in this order)

1. `just linter` — runs `ruff format` (rewrites files in place), then `ruff check --exit-non-zero-on-fix` (reports fixable issues without applying them), then codespell (alias: `just lint`). Expect formatting changes in your working tree after running it.
2. `just mypy` — strict mode over `faststream/` and `tests/mypy/`.
3. `just static-analysis` — mypy + bandit + semgrep; `just zizmor` separately for GitHub Actions workflows.
4. `just pre-commit` — pre-commit hooks on modified files (`just pre-commit-all` for the whole tree).

## Docs recipes

- `just docs-serve` — live server; `just docs-build` — static build (details in the **documentation-writing** skill).

## CI expectations

- CI (`.github/workflows/pr_tests.yaml`): core jobs run everything except `connected` (`-m "(slow and not connected) or not connected"`); per-broker jobs run `<broker> and not connected`, plus dedicated jobs with real broker services for `connected` tests.
- 30s per-test timeout; xdist parallelism — tests must be order-independent.
- Coverage sources include `faststream/`, `tests/`, `docs/docs_src/`, and `examples/`.

## Related skills

- **testing-patterns** — markers, base testcases, fixtures.
- **code-architecture** — source conventions the linters enforce.
- **documentation-writing** — docs authoring and snippet testing.
