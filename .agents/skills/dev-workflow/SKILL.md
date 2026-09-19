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
2. `just mypy` — strict mode over `faststream/` and all of `tests/`; `docs/docs_src/` with the annotation checks relaxed.
3. `just static-analysis` — mypy + bandit + semgrep; `just zizmor` separately for GitHub Actions workflows.
4. `just pre-commit` — pre-commit hooks on modified files (`just pre-commit-all` for the whole tree).

## Docs recipes

- `just docs-serve` — live server; `just docs-build` — static build; `just docs-check` — the docs PR gate (details in the **documentation-writing** skill).

## CI expectations

- CI (`.github/workflows/pr_tests.yaml`): core jobs run everything except `connected` (`-m "(slow and not connected) or not connected"`); per-broker jobs run `<broker> and not connected`, plus dedicated jobs with real broker services for `connected` tests.
- 30s per-test timeout; xdist parallelism — tests must be order-independent.
- Coverage sources include `faststream/`, `tests/`, `docs/docs_src/`, and `examples/`.

## Branches and PRs

- **List the open PRs before continuing earlier work**: `gh pr list --state open`, and read the titles in the same area. Merged history shows what shipped, not what is in flight (#3150 redid what open #3137 already had).
- **The main checkout is shared between sessions.** Run `git branch --show-current` right before each commit, and do multi-step branch work in a `git worktree` under `.claude/worktrees/`.
- **Rewriting a PR's commits**: rebase onto the fresh `origin/main` first, then `reset --soft origin/main` and recommit. A soft reset straight onto a main that has moved keeps the old tree, and the new commit reverts whatever merged in between. Before pushing, `git diff --stat origin/main` lists only the PR's own files.
- **Stacked PRs** use the `gh stack` extension. `gh stack link <stack> <pr>` puts a PR in a stack — a base branch alone does not. Merge bottom-up with `gh pr merge <n> --squash`, then `gh stack sync`. Linking can flip a PR to draft, and every test job skips drafts: check `gh pr view <n> --json isDraft`. Once one PR of a stack has merged the stack cannot grow; form a new one from the open PRs with `gh stack link --remote origin <pr> <pr> ...`, bottom to top.
- A force-push to a PR branch waits for the maintainer's word; `main` dismisses stale approvals on every push.

## Related skills

- **testing-patterns** — markers, base testcases, fixtures.
- **code-architecture** — source conventions the linters enforce.
- **documentation-writing** — docs authoring and snippet testing.
