[doc("All command information")]
default:
  @just --list --unsorted --list-heading $'FastStream  commands…\n'


# Infra
[doc("Init infra")]
[group("infra")]
init python="3.10":
  docker build . --build-arg PYTHON_VERSION={{python}}

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
coverage-test path="tests/" params="" marks="not slow and not kafka and not confluent and not redis and not rabbit and not nats":
  -docker compose exec faststream uv run sh -c "coverage run -m pytest {{path}} -m '{{marks}}' {{params}} && coverage combine && coverage report --show-missing --skip-covered --sort=cover --precision=2 && rm .coverage*"

[doc("Run all tests with coverage")]
[group("tests")]
coverage-test-all path="tests/" params="" marks="all":
  -docker compose exec faststream uv run sh -c "coverage run -m pytest {{path}} -m '{{marks}}' {{params}} && coverage combine && coverage report --show-missing --skip-covered --sort=cover --precision=2 && rm .coverage*"


# Docs
[doc("Build docs")]
[group("docs")]
docs-build:
  docker compose exec -T faststream uv run sh -c "cd docs && python docs.py build"

[doc("Serve docs")]
[group("docs")]
docs-serve:
  docker compose exec faststream uv run sh -c "cd docs && mkdocs serve"


# Linter
[doc("Ruff check")]
[group("linter")]
ruff-check:
  -docker compose exec -T faststream uv run ruff check --exit-non-zero-on-fix

[doc("Ruff format")]
[group("linter")]
ruff-format:
  -docker compose exec -T faststream uv run ruff format

[doc("Codespell check")]
[group("linter")]
codespell:
  -docker compose exec -T faststream uv run codespell

[doc("Linter run")]
[group("linter")]
linter: ruff-check ruff-format codespell


# Static analysis
[doc("Mypy check")]
[group("static analysis")]
mypy:
  -docker compose exec -T faststream uv run mypy

[doc("Bandit check")]
[group("static analysis")]
bandit:
  -docker compose exec -T faststream uv run bandit -c pyproject.toml -r faststream

[doc("Semgrep check")]
[group("static analysis")]
semgrep:
  -docker compose exec -T faststream uv run semgrep scan --config auto --error

[doc("Static analysis check")]
[group("static analysis")]
static-analysis: mypy bandit semgrep

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
