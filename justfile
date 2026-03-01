# Cross-platform shell configuration
# Use PowerShell on Windows (higher precedence than shell setting)
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
# Use sh on Unix-like systems
set shell := ["sh", "-c"]

export VIRTUAL_ENV := ".venv"  # this requires a pre-commit


[doc("All command information")]
default:
  @just --list --unsorted --list-heading $'FastStream  commands…\n'


# Infra
[doc("Init infra")]
[group("infra")]
init python="3.10":
  docker build . --build-arg PYTHON_VERSION={{python}}
  uv sync --group dev -p {{python}}

[doc("Run all containers")]
[group("infra")]
up:
  docker compose up -d

[doc("Stop all containers")]
[group("infra")]
stop:
  docker compose stop

[doc("Down all containers")]
[group("infra")]
down:
  docker compose down


[doc("Run fast tests")]
[group("tests")]
test +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "not slow and not connected" -n auto

[doc("Run all tests")]
[group("tests")]
test-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "all" -n auto

[doc("Run fast tests with coverage")]
[group("tests")]
test-coverage +param="tests/":
  -docker compose exec faststream uv run sh -c "pytest {{param}} -m 'not slow and not connected' -n auto --cov --cov-report=term:skip-covered"

[doc("Run all tests with coverage")]
[group("tests")]
test-coverage-all +param="tests/":
  -docker compose exec faststream uv run sh -c "pytest {{param}} -m 'all' -n auto --cov --cov-report=term:skip-covered"


# Docs
_docs *params:
  cd docs && uv run --frozen python docs.py {{params}}

[doc("Build docs")]
[group("docs")]
docs-build:
  just _docs build

[doc("Build API Reference")]
[group("docs")]
docs-build-api:
  just _docs build-api-docs

[doc("Update release notes")]
[group("docs")]
docs-update-release-notes:
  just _docs update-release-notes

[doc("Serve docs")]
[group("docs")]
docs-serve params="":
  just _docs live 8000 {{params}}


# Linter
_linter *params:
  uv run --no-dev --group lint --frozen {{params}}

[doc("Ruff format")]
[group("linter")]
ruff-format *params:
  just _linter ruff format {{params}}

[doc("Ruff check")]
[group("linter")]
ruff-check *params:
  just _linter ruff check --exit-non-zero-on-fix {{params}}

_codespell:
  just _linter codespell -L Dependant,dependant --skip "./docs/site"

[doc("Check typos")]
[group("linter")]
typos: _codespell
  just _linter pre-commit run --all-files typos

alias lint := linter

[doc("Linter run")]
[group("linter")]
linter: ruff-format ruff-check _codespell


# Static analysis
_static *params:
  uv run --frozen {{params}}

[doc("Mypy check")]
[group("static analysis")]
mypy *params:
  just _static mypy {{params}}

[doc("Bandit check")]
[group("static analysis")]
bandit:
  just _static bandit -c pyproject.toml -r faststream

[doc("Semgrep check")]
[group("static analysis")]
semgrep:
  just _static semgrep scan --config auto --error --skip-unknown-extensions faststream

[doc("Zizmor check")]
[group("static analysis")]
zizmor:
  just _static zizmor .

[doc("Static analysis check")]
[group("static analysis")]
static-analysis: mypy bandit semgrep


# Pre-commit
_pre_commit *params:
  uv run --frozen pre-commit {{params}}

[doc("Install pre-commit hooks")]
[group("pre-commit")]
pre-commit-install:
  just _pre_commit install

[doc("Pre-commit modified files")]
[group("pre-commit")]
pre-commit:
  just _pre_commit run

[doc("Pre-commit all files")]
[group("pre-commit")]
pre-commit-all:
  just _pre_commit run --all-files


# Kafka
[doc("Run kafka container")]
[group("kafka")]
kafka-up:
  docker compose up -d kafka

[doc("Stop kafka container")]
[group("kafka")]
kafka-stop:
  docker compose stop kafka

[doc("Show kafka logs")]
[group("kafka")]
kafka-logs:
  docker compose logs -f kafka

[doc("Run kafka memory tests")]
[group("kafka")]
[group("tests")]
test-kafka +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "kafka and not connected and not slow" -n auto

[doc("Run kafka all tests")]
[group("kafka")]
[group("tests")]
test-kafka-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "kafka or (kafka and slow)" -n auto

[doc("Run confluent memory tests")]
[group("kafka")]
[group("tests")]
test-confluent +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "confluent and not connected and not slow" -n auto

[doc("Run confluent all tests")]
[group("confluent")]
[group("tests")]
test-confluent-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "confluent or (confluent and slow)" -n auto


# RabbitMQ
[doc("Run rabbitmq container")]
[group("rabbitmq")]
rabbit-up:
  docker compose up -d rabbitmq

[doc("Stop rabbitmq container")]
[group("rabbitmq")]
rabbit-stop:
  docker compose stop rabbitmq

[doc("Show rabbitmq logs")]
[group("rabbitmq")]
rabbit-logs:
  docker compose logs -f rabbitmq

[doc("Run rabbitmq memory tests")]
[group("rabbitmq")]
[group("tests")]
test-rabbit +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "rabbit and not connected and not slow" -n auto

[doc("Run rabbitmq all tests")]
[group("rabbitmq")]
[group("tests")]
test-rabbit-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "rabbit or (rabbit and slow)" -n auto


# Redis
[doc("Run redis container")]
[group("redis")]
redis-up:
  docker compose up -d redis

[doc("Stop redis container")]
[group("redis")]
redis-stop:
  docker compose stop redis

[doc("Show redis logs")]
[group("redis")]
redis-logs:
  docker compose logs -f redis

[doc("Run redis memory tests")]
[group("redis")]
[group("tests")]
test-redis +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "redis and not connected and not slow" -n auto

[doc("Run redis all tests")]
[group("redis")]
[group("tests")]
test-redis-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "redis or (redis and slow)" -n auto


# Nats
[doc("Run nats container")]
[group("nats")]
nats-up:
  docker compose up -d nats

[doc("Stop nats container")]
[group("nats")]
nats-stop:
  docker compose stop nats

[doc("Show nats logs")]
[group("nats")]
nats-logs:
  docker compose logs -f nats

[doc("Run nats memory tests")]
[group("nats")]
[group("tests")]
test-nats +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "nats and not connected and not slow" -n auto

[doc("Run nats all tests")]
[group("nats")]
[group("tests")]
test-nats-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "nats or (nats and slow)" -n auto

[doc("Run benchmarks")]
[group("benchmarks")]
bench:
  cd benchmarks && uv run --active --frozen python bench.py
