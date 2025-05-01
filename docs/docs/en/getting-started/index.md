---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
hide:
  - toc
---

# QUICK START

Install using `pip`:

=== "AIOKafka"
    ```console
    pip install "faststream[kafka]"
    ```

    !!! tip
        To start a new project, we need a test broker container
        ```bash
        docker run -d --rm -p 9092:9092 --name test-mq \
        -e KAFKA_ENABLE_KRAFT=yes \
        -e KAFKA_CFG_NODE_ID=1 \
        -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
        -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
        -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
        -e KAFKA_BROKER_ID=1 \
        -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e ALLOW_PLAINTEXT_LISTENER=yes \
        bitnami/kafka:3.5.0
        ```

=== "Confluent"
    ```console
    pip install "faststream[confluent]"
    ```

    !!! tip
        To start a new project, we need a test broker container
        ```bash
        docker run -d --rm -p 9092:9092 --name test-mq \
        -e KAFKA_ENABLE_KRAFT=yes \
        -e KAFKA_CFG_NODE_ID=1 \
        -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
        -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
        -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
        -e KAFKA_BROKER_ID=1 \
        -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e ALLOW_PLAINTEXT_LISTENER=yes \
        bitnami/kafka:3.5.0
        ```

=== "RabbitMQ"
    ```console
    pip install "faststream[rabbit]"
    ```

    !!! tip
        To start a new project, we need a test broker container
        ```bash
        docker run -d --rm -p 5672:5672 --name test-mq rabbitmq:alpine
        ```


=== "NATS"
    ```console
    pip install "faststream[nats]"
    ```

    !!! tip
        To start a new project, we need a test broker container
        ```bash
        bash docker run -d --rm -p 4222:4222 --name test-mq nats -js
        ```

=== "Redis"
    ```console
    pip install "faststream[redis]"
    ```

    !!! tip
        To start a new project, we need a test broker container
        ```bash
        bash docker run -d --rm -p 6379:6379 --name test-mq redis
        ```

## Basic Usage

!!! note
    Before continuing with the next steps, make sure you install *Fastream* CLI.
    ```shell
    pip install "faststream[cli]"
    ```

To create a basic application, add the following code to a new file (e.g. `serve.py`):

=== "AIOKafka"
    ```python linenums="1" title="serve.py"
    {!> docs_src/getting_started/index/base_kafka.py!}
    ```

=== "Confluent"
    ```python linenums="1" title="serve.py"
    {!> docs_src/getting_started/index/base_confluent.py!}
    ```

=== "RabbitMQ"
    ```python linenums="1" title="serve.py"
    {!> docs_src/getting_started/index/base_rabbit.py!}
    ```

=== "NATS"
    ```python linenums="1" title="serve.py"
    {!> docs_src/getting_started/index/base_nats.py!}
    ```

=== "Redis"
    ```python linenums="1" title="serve.py"
    {!> docs_src/getting_started/index/base_redis.py!}
    ```


And just run this command:

```shell
faststream run serve:app
```

After running the command, you should see the following output:

```{.shell .no-copy}
INFO     - FastStream app starting...
INFO     - test |            - `BaseHandler` waiting for messages
INFO     - FastStream app started successfully! To exit, press CTRL+C
```
{ data-search-exclude }

Enjoy your new development experience!

### Manual run

Also, you can run the `FastStream` application manually, as a regular async function:

```python
import asyncio

async def main():
    app = FastStream(...)
    await app.run()  # blocking method

if __name__ == "__main__":
    asyncio.run(main())
```

### Other tools integrations

If you want to use **FastStream** as part of another framework service, you probably don't need to utilize the `FastStream` object at all, as it is primarily intended as a **CLI** tool target. Instead, you can start and stop your broker as part of another framework's lifespan. You can find such examples in the [integrations section](./integrations/frameworks/index.md){.internal-link}.

??? tip "Don't forget to stop the test broker container"
    ```bash
    docker container stop test-mq
    ```
