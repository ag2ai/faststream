# Cross-platform shell configuration
# Use PowerShell on Windows (higher precedence than shell setting)
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
# Use sh on Unix-like systems
set shell := ["sh", "-c"]


[doc("All command information")]
default:
  @just --list --unsorted --list-heading $'FastStream  commands…\n'


# Infra
[doc("Init infra")]
[group("infra")]
init python="3.10":
  docker build . --build-arg PYTHON_VERSION={{python}}
  uv sync --group dev

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
  docker compose exec faststream uv run pytest {{param}} -m "not slow and not connected"

[doc("Run all tests")]
[group("tests")]
test-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "all"

[doc("Run fast tests with coverage")]
[group("tests")]
test-coverage +param="tests/":
  -docker compose exec faststream uv run sh -c "coverage run -m pytest {{param}} -m 'not slow and not connected' && coverage combine && coverage report --show-missing --skip-covered --sort=cover --precision=2 && rm .coverage*"

[doc("Run all tests with coverage")]
[group("tests")]
test-coverage-all +param="tests/":
  -docker compose exec faststream uv run sh -c "coverage run -m pytest {{param}} -m 'all' && coverage combine && coverage report --show-missing --skip-covered --sort=cover --precision=2 && rm .coverage*"


# Docs
[doc("Build docs")]
[group("docs")]
docs-build:
  cd docs && uv run python docs.py build

[doc("Build API Reference")]
[group("docs")]
docs-build-api:
  cd docs && uv run python docs.py build-api-docs

[doc("Update release notes")]
[group("docs")]
docs-update-release-notes:
  cd docs && uv run python docs.py update-release-notes

[doc("Serve docs")]
[group("docs")]
docs-serve params="":
  cd docs && uv run python docs.py live 8000 {{params}}

# Linter
[doc("Ruff format")]
[group("linter")]
ruff-format *params:
  uv run --active ruff format {{params}}

[doc("Ruff check")]
[group("linter")]
ruff-check *params:
  uv run --active ruff check --exit-non-zero-on-fix {{params}}

_codespell:
  uv run --active codespell -L Dependant,dependant --skip "./docs/site"

[doc("Check typos")]
[group("linter")]
typos: _codespell
  uv run pre-commit run --all-files typos

alias lint := linter

[doc("Linter run")]
[group("linter")]
linter: ruff-format ruff-check _codespell

# Static analysis
[doc("Mypy check")]
[group("static analysis")]
mypy *params:
  uv run mypy {{params}}

[doc("Bandit check")]
[group("static analysis")]
bandit:
  uv run bandit -c pyproject.toml -r faststream

[doc("Semgrep check")]
[group("static analysis")]
semgrep:
  uv run semgrep scan --config auto --error --skip-unknown-extensions faststream

[doc("Static analysis check")]
[group("static analysis")]
static-analysis: mypy bandit semgrep

[doc("Install pre-commit hooks")]
[group("pre-commit")]
pre-commit-install:
  uv run pre-commit install

[doc("Pre-commit modified files")]
[group("pre-commit")]
pre-commit:
  uv run pre-commit run

[doc("Pre-commit all files")]
[group("pre-commit")]
pre-commit-all:
  uv run pre-commit run --all-files

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
  docker compose exec faststream uv run pytest {{param}} -m "kafka and not connected and not slow"

[doc("Run kafka all tests")]
[group("kafka")]
[group("tests")]
test-kafka-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "kafka or (kafka and slow)"

[doc("Run confluent memory tests")]
[group("kafka")]
[group("tests")]
test-confluent +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "confluent and not connected and not slow"

[doc("Run confluent all tests")]
[group("confluent")]
[group("tests")]
test-confluent-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "confluent or (confluent and slow)"


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
  docker compose exec faststream uv run pytest {{param}} -m "rabbit and not connected and not slow"

[doc("Run rabbitmq all tests")]
[group("rabbitmq")]
[group("tests")]
test-rabbit-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "rabbit or (rabbit and slow)"


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
  docker compose exec faststream uv run pytest {{param}} -m "redis and not connected and not slow"

[doc("Run redis all tests")]
[group("redis")]
[group("tests")]
test-redis-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "redis or (redis and slow)"


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
  docker compose exec faststream uv run pytest {{param}} -m "nats and not connected and not slow"

[doc("Run nats all tests")]
[group("nats")]
[group("tests")]
test-nats-all +param="tests/":
  docker compose exec faststream uv run pytest {{param}} -m "nats or (nats and slow)"

[doc("Run benchmarks")]
[group("benchmarks")]
bench:
  cd benchmarks && uv run python bench.py
