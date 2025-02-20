[doc("All command information")]
default:
  @just --list --unsorted --list-heading $'FastStream  commands…\n'


# Infra
[doc("Init infra")]
[group("infra")]
init python="3.8":
  docker build . --build-arg PYTHON_VERSION={{python}}
  python -m venv venv

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


[doc("Run tests")]
[group("tests")]
test target="all":
  docker compose exec faststream pytest -m "{{target}}"

[doc("Run all tests")]
[group("tests")]
coverage-test target="all":
  -docker compose exec faststream sh -c "coverage run -m pytest -m '{{target}}' && coverage combine && coverage report --show-missing --skip-covered --sort=cover --precision=2 && rm .coverage*"


# Docs
[doc("Build docs")]
[group("docs")]
docs-build:
  docker compose exec -T faststream sh -c "cd docs && python docs.py build"

[doc("Serve docs")]
[group("docs")]
docs-serve:
  docker compose exec faststream sh -c "cd docs && python docs.py live"


# Linter
[doc("Ruff check")]
[group("linter")]
ruff-check:
  -ruff check --exit-non-zero-on-fix

[doc("Ruff format")]
[group("linter")]
ruff-format:
  -ruff format

[doc("Codespell check")]
[group("linter")]
codespell:
  -codespell

[doc("Linter run")]
[group("linter")]
linter: ruff-check ruff-format codespell


# Static analysis
[doc("Mypy check")]
[group("static analysis")]
mypy:
  -mypy

[doc("Bandit check")]
[group("static analysis")]
bandit:
  -bandit -c pyproject.toml -r faststream

[doc("Semgrep check")]
[group("static analysis")]
semgrep:
  -semgrep scan --config auto --error

[doc("Static analysis check")]
[group("static analysis")]
static-analysis: mypy bandit semgrep


# Pre-commit
[doc("Pre-commit install")]
[group("pre-commit")]
pre-commit-install:
  pip install pre-commit
  pre-commit install

[doc("Pre-commit run")]
[group("pre-commit")]
pre-commit:
  -pre-commit run --all


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

[doc("Run kafka tests")]
[group("kafka")]
kafka-tests: (test "kafka")


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

[doc("Run rabbitmq tests")]
[group("rabbitmq")]
rabbit-tests: (test "rabbit")


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

[doc("Run redis tests")]
[group("redis")]
redis-tests: (test "redis")


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

[doc("Run nats tests")]
[group("nats")]
nats-tests: (test "nats")
