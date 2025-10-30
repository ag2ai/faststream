---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Healthchecks

A common pattern for healthchecks is a low-cost **HTTP** endpoint,
**FastStream** implements this feature and allows you to create liveness and readiness probes using [ASGI](../asgi.md) feature.

### Liveness Probe

Liveness probes determine when to restart a container.
For example: liveness probes could catch a deadlock when an application is running but unable to make progress.

### Readiness Probe

Readiness probes determine when a container is ready to start accepting traffic.
This is useful when waiting for an application to perform time-consuming initial tasks, such as establishing network connections, loading files, and warming caches.

### Let's implement readiness and liveness probes

Structure of our app:

```text
dummy
├── docker-compose.yml
├── Dockerfile
├── main.py
```

This example shows how to implement liveness and readiness probes in **FastStream**.
Readiness probe checks connection to **Redis**, **RabbitMQ** and **Postgres**. Liveness probe checks that app just works.

```python linenums="1" hl_lines="13-15 23 24 32 38 44 46 61-64 67" title="main.py"
import asyncio
import logging

import asyncpg
import redis.asyncio as redis
import uvicorn
from faststream import FastStream
from faststream.asgi import AsgiResponse, get
from faststream.asgi.types import ASGIApp, Scope
from faststream.rabbit import RabbitBroker


@get
async def liveness(scope: Scope) -> AsgiResponse:
    return AsgiResponse(b"", status_code=204)


def readiness(
    broker: RabbitBroker,
    redis_connection: redis.Redis,
    postgres_connection: asyncpg.Pool,
) -> ASGIApp:
    healthy_response = AsgiResponse(b"", 204)
    unhealthy_response = AsgiResponse(b"", 500)

    @get
    async def func(scope: Scope) -> AsgiResponse:
        try:
            await redis_connection.ping()
        except (redis.ConnectionError, Exception):
            logging.exception("Redis not ready")
            return unhealthy_response

        try:
            await broker.ping(timeout=5.0)
        except Exception:
            logging.exception("RabbitMQ not ready")
            return unhealthy_response

        try:
            await postgres_connection.fetchval("SELECT 1")
        except (asyncpg.exceptions.PostgresConnectionError, Exception):
            logging.exception("Postgres not ready")
            return unhealthy_response

        return healthy_response

    return func


async def main() -> None:
    redis_connection = redis.Redis(host="redis", port=6379)

    postgres_connection = await asyncpg.create_pool(
        "postgresql://user:password@postgres/postgres"
    )

    broker = RabbitBroker("amqp://guest:guest@rabbitmq:5672/")
    app = FastStream(broker)

    asgi_routes = [
        ("/internal/alive", liveness),
        ("/internal/ready", readiness(broker, redis_connection, postgres_connection)),
    ]

    uvicorn_config = uvicorn.Config(
        app.as_asgi(asgi_routes),
        host="0.0.0.0",
        port=8000,
    )
    server = uvicorn.Server(uvicorn_config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
```

### Pack to Dockerfile

And let's build a simple **Dockerfile** for our app:

```dockerfile linenums="1" title="Dockerfile"
FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl

WORKDIR /app
RUN pip install 'faststream[rabbit]' uvicorn redis asyncpg
COPY main.py /app
```

### Docker-compose with healtcheck

**Docker-compose** doesn't allow you to realize the full power of trials, but it's enough to increase the stability of your application.

```yaml linenums="1" hl_lines="37"
services:
  rabbitmq:
    image: rabbitmq:3.8-management
    container_name: rabbitmq-local
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    ports:
      - "15672:15672"
    healthcheck:
      test: [ "CMD", "rabbitmq-diagnostics", "-q", "ping"]

  redis:
    image: redis:7.2
    container_name: redis-local
    healthcheck:
      test: [ "CMD", "redis-cli", "--raw", "incr", "ping" ]

  postgres:
    image: postgres:14
    container_name: postgres-local
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    healthcheck:
      test: [ "CMD", "pg_isready", "-U", "user" ]

  app:
    build: .
    container_name: app-local
    command: bash -c "python main.py"
    ports:
      - "8000:8000"
    volumes:
      - ./main.py:/app/main.py
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/internal/ready"]
      interval: 60s
      timeout: 5s
      retries: 5
      start_period: 5s
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
```

### Kubernetes deployment

But if you use k8s you can use the full power of this feature because you can use live and ready probes together.
This is an example of deployment with liveness and readiness probes:

```yaml title="faststream-deployment.yaml" linenums="1" hl_lines="23-26 29-32"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dummy-app
  labels:
    app: dummy-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dummy-app
  template:
    metadata:
      labels:
        app: dummy-app
    spec:
      containers:
        - name: dummy-app
          image: "dummy-app:latest"
          command: ["/bin/sh", "-c", 'python3 main.py']
          ports:
            - containerPort: 8000
          livenessProbe:
            httpGet:
              path: /internal/alive
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 20
          readinessProbe:
            httpGet:
              path: /internal/ready
              port: 8000
            periodSeconds: 60
```
