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
test path="tests/" params="" marks="not slow and not kafka and not confluent and not redis and not rabbit and not nats":
  docker compose exec faststream uv run pytest {{path}} -m "{{marks}}" {{params}}

[doc("Run all tests")]
[group("tests")]
test-all path="tests/" params="" marks="all":
  docker compose exec faststream uv run pytest {{path}} -m "{{marks}}" {{params}}

[doc("Run fast tests with coverage")]
[group("tests")]
test-coverage path="tests/" params="" marks="not slow and not kafka and not confluent and not redis and not rabbit and not nats":
  -docker compose exec faststream uv run sh -c "coverage run -m pytest {{path}} -m '{{marks}}' {{params}} && coverage combine && coverage report --show-missing --skip-covered --sort=cover --precision=2 && rm .coverage*"

[doc("Run all tests with coverage")]
[group("tests")]
test-coverage-all path="tests/" params="" marks="all":
  -docker compose exec faststream uv run sh -c "coverage run -m pytest {{path}} -m '{{marks}}' {{params}} && coverage combine && coverage report --show-missing --skip-covered --sort=cover --precision=2 && rm .coverage*"


# Docs
[doc("Build docs")]
[group("docs")]
docs-build:
  cd docs && uv run python docs.py build

[doc("Serve docs")]
[group("docs")]
docs-serve:
  cd docs && uv run python docs.py live 8000 --fast


# Linter
[doc("Ruff check")]
[group("linter")]
ruff-check *params:
  uv run ruff check --exit-non-zero-on-fix {{params}}

[doc("Ruff format")]
[group("linter")]
ruff-format *params:
  uv run ruff format {{params}}

[doc("Codespell check")]
[group("linter")]
codespell:
  uv run codespell

alias lint := linter

[doc("Linter run")]
[group("linter")]
linter: ruff-format ruff-check codespell


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
  uv run semgrep scan --config auto --error

[doc("Static analysis check")]
[group("static analysis")]
static-analysis: mypy bandit semgrep


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

[doc("Run kafka tests")]
[group("kafka")]
test-kafka: (test "tests/brokers/kafka")

[doc("Run confluent tests")]
[group("kafka")]
test-confluent: (test "tests/brokers/confluent")


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
test-rabbit: (test "tests/brokers/rabbit")


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
test-redis: (test "tests/brokers/redis")


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
test-nats *params: (test "tests/brokers/nats")
