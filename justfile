[doc('All command information')]
default:
  @just --list --unsorted --list-heading $'FastStream  commands…\n'


# Infra
[doc('Run all containers')]
[group('infra')]
fs-up:
  docker compose up -d

[doc('Stop all containers')]
[group('infra')]
fs-stop:
  docker compose stop

[doc('Down all containers')]
[group('infra')]
fs-down:
  docker compose down


# Kafka
[doc('Run kafka container')]
[group('kafka')]
kafka-up:
  docker compose up -d kafka

[doc('Stop kafka container')]
[group('kafka')]
kafka-stop:
  docker compose stop kafka

[doc('Show kafka logs')]
[group('kafka')]
kafka-logs:
  docker compose logs -f kafka

[doc('Run kafka tests')]
[group('kafka')]
kafka-tests: kafka-up && kafka-stop
  -docker compose run --rm faststream pytest -m 'kafka'


# RabbitMQ
[doc('Run rabbitmq container')]
[group('rabbitmq')]
rabbit-up:
  docker compose up -d rabbitmq

[doc('Stop rabbitmq container')]
[group('rabbitmq')]
rabbit-stop:
  docker compose stop rabbitmq

[doc('Show rabbitmq logs')]
[group('rabbitmq')]
rabbit-logs:
  docker compose logs -f rabbitmq

[doc('Run rabbitmq tests')]
[group('rabbitmq')]
rabbit-tests: rabbit-up && rabbit-stop
  -docker compose run --rm faststream pytest -m 'rabbit'


# Redis
[doc('Run redis container')]
[group('redis')]
redis-up:
  docker compose up -d redis

[doc('Stop redis container')]
[group('redis')]
redis-stop:
  docker compose stop redis

[doc('Show redis logs')]
[group('redis')]
redis-logs:
  docker compose logs -f redis

[doc('Run redis tests')]
[group('redis')]
redis-tests: redis-up && redis-stop
  -docker compose run --rm faststream pytest -m 'redis'


# Nats
[doc('Run nats container')]
[group('nats')]
nats-up:
  docker compose up -d nats

[doc('Stop nats container')]
[group('nats')]
nats-stop:
  docker compose stop nats

[doc('Show nats logs')]
[group('nats')]
nats-logs:
  docker compose logs -f nats

[doc('Run nats tests')]
[group('nats')]
nats-tests: nats-up && nats-stop
  -docker compose run --rm faststream pytest -m 'nats'
