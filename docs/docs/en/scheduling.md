---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Tasks Scheduling

**FastStream** is a framework for asynchronous service development. It allows you to build distributed event-based systems in an easy way. Task scheduling is a pretty common use case in such systems.

Unfortunately, this functionality conflicts with the original **FastStream** ideology and can't be implemented as a part of the framework. But, you can integrate scheduling in your **FastStream** application by using some extra dependencies. And we have some recipes on how to do it.

## Taskiq-FastStream

[**Taskiq**](https://github.com/taskiq-python/taskiq){.external-link target="_blank"} is an asynchronous distributed task queue for Python. This project takes inspiration from big projects such as **Celery** and **Dramatiq**.

As a **Celery** replacement, **Taskiq** should support task scheduling and delayed publishing, of course. And it does!

By the way, you can easily integrate **FastStream** with **Taskiq**. It allows you to create cron or delayed tasks to publish messages and trigger some functions this way.

We have a helpful project to provide you with this feature - [**Taskiq-FastStream**](https://github.com/taskiq-python/taskiq-faststream){.external-link target="_blank"}.

!!! note ""
    You can install it by the following command
    ```bash
    pip install taskiq-faststream
    ```

It has two helpful classes `BrokerWrapper` and `AppWrapper` to make your **FastStream** App and Broker objects *taskiq-compatible*.

Let's take a look at the code example.

At first, we should create a regular **FastStream** application.

=== "AIOKafka"
    ```python linenums="1"
    {!> docs_src/index/kafka/basic.py!}
    ```

=== "Confluent"
    ```python linenums="1"
    {!> docs_src/index/confluent/basic.py!}
    ```

=== "RabbitMQ"
    ```python linenums="1"
    {!> docs_src/index/rabbit/basic.py!}
    ```

=== "NATS"
    ```python linenums="1"
    {!> docs_src/index/nats/basic.py!}
    ```

=== "Redis"
    ```python linenums="1"
    {!> docs_src/index/redis/basic.py!}
    ```

=== "MQTT"
    ```python linenums="1"
    {!> docs_src/index/mqtt/basic.py!}
    ```

### Broker Wrapper

Now, if you want to make it *just work*, we should wrap our `Broker` in a special `BrokerWrapper` object:

```python
from taskiq_faststream import BrokerWrapper

taskiq_broker = BrokerWrapper(broker)
```

It creates a *taskiq-compatible* object that can be used as an object to create a regular [**taskiq** scheduler](https://taskiq-python.github.io/guide/scheduling-tasks.html){.external-link target="_blank"}.

=== "AIOKafka"
    ```python linenums="1"
    from taskiq_faststream import StreamScheduler
    from taskiq.schedule_sources import LabelScheduleSource

    taskiq_broker.task(
        message={"user": "John", "user_id": 1},
        topic="in-topic",
        schedule=[{
            "cron": "* * * * *",
        }],
    )

    scheduler = StreamScheduler(
        broker=taskiq_broker,
        sources=[LabelScheduleSource(taskiq_broker)],
    )
    ```

=== "Confluent"
    ```python linenums="1"
    from taskiq_faststream import StreamScheduler
    from taskiq.schedule_sources import LabelScheduleSource

    taskiq_broker.task(
        message={"user": "John", "user_id": 1},
        topic="in-topic",
        schedule=[{
            "cron": "* * * * *",
        }],
    )

    scheduler = StreamScheduler(
        broker=taskiq_broker,
        sources=[LabelScheduleSource(taskiq_broker)],
    )
    ```

=== "RabbitMQ"
    ```python linenums="1"
    from taskiq_faststream import StreamScheduler
    from taskiq.schedule_sources import LabelScheduleSource

    taskiq_broker.task(
        message={"user": "John", "user_id": 1},
        queue="in-queue",
        schedule=[{
            "cron": "* * * * *",
        }],
    )

    scheduler = StreamScheduler(
        broker=taskiq_broker,
        sources=[LabelScheduleSource(taskiq_broker)],
    )
    ```

=== "NATS"
    ```python linenums="1"
    from taskiq_faststream import StreamScheduler
    from taskiq.schedule_sources import LabelScheduleSource

    taskiq_broker.task(
        message={"user": "John", "user_id": 1},
        subject="in-subject",
        schedule=[{
            "cron": "* * * * *",
        }],
    )

    scheduler = StreamScheduler(
        broker=taskiq_broker,
        sources=[LabelScheduleSource(taskiq_broker)],
    )
    ```

=== "Redis"
    ```python linenums="1"
    from taskiq_faststream import StreamScheduler
    from taskiq.schedule_sources import LabelScheduleSource

    taskiq_broker.task(
        message={"user": "John", "user_id": 1},
        channel="in-channel",
        schedule=[{
            "cron": "* * * * *",
        }],
    )

    scheduler = StreamScheduler(
        broker=taskiq_broker,
        sources=[LabelScheduleSource(taskiq_broker)],
    )
    ```

=== "MQTT"
    ```python linenums="1"
    from taskiq_faststream import StreamScheduler
    from taskiq.schedule_sources import LabelScheduleSource

    taskiq_broker.task(
        message={"user": "John", "user_id": 1},
        topic="in-topic",
        schedule=[{
            "cron": "* * * * *",
        }],
    )

    scheduler = StreamScheduler(
        broker=taskiq_broker,
        sources=[LabelScheduleSource(taskiq_broker)],
    )
    ```


!!! note ""
    We patched the original `TaskiqScheduler` to support message generation callbacks, but its signature remains the same.

`#!python broker.task(...)` has the same signature as the original `broker.publish(...)` and allows you to plan your publishing tasks using the great **taskiq** `schedule` option (you can learn more about it [here](https://taskiq-python.github.io/available-components/schedule-sources.html#labelschedulesource){.external-link target="_blank"}).

Finally, to run the scheduler, please use the **taskiq CLI** command:

```bash
taskiq scheduler module:scheduler
```

### Application Wrapper

If you don't want to lose application **AsyncAPI** schema and/or lifespans, you can wrap not the broker, but the application itself using `AppWrapper` class.

```python
from taskiq_faststream import AppWrapper

taskiq_broker = AppWrapper(app)
```

It allows you to use `taskiq_broker` the same way as in the previous example, but preserves all original **FastStream** features.

!!! tip
    Creating a separate *Scheduler* service is the best way to make a really distributed and sustainable system. In this case, you can just create an empty **FastStream** broker and use **Taskiq-FastStream** integration to publish your messages (consumed by other services).

### Generate message payload

Also, you are able to determine the message payload right before sending instead of using a final one. To do it, just replace the final value of the `message` option with a function (sync or async) that returns the data to send:

```python
async def collect_information_to_send():
    return "Message to send"

taskiq_broker.task(
    message=collect_information_to_send,
    ...
)
```

It allows you to collect some data from a database, request an external API, or use other ways to generate data to send right before sending.

Moreover, you can send not one, but multiple messages per task using this feature. Just turn your message callback function into a generator (sync or async) - and **Taskiq-FastStream** will iterate over your payload and publish all of your messages!

```python
async def collect_information_to_send():
    """Publish 10 messages per task call."""
    for i in range(10):
        yield i

taskiq_broker.task(
    message=collect_information_to_send,
    ...
)
```

## Rocketry

Also, you can integrate your **FastStream** application with any other library that provides you with scheduling functionality.

As an example, you can use [**Rocketry**](https://github.com/Miksus/rocketry){.external-link target="_blank"}:

```python linenums="1"
import asyncio

from rocketry import Rocketry
from rocketry.args import Arg

from faststream.nats import NatsBroker

app = Rocketry(execution="async")

broker = NatsBroker()      # regular broker
app.params(broker=broker)

async def start_app():
    async with broker:     # connect broker
        await app.serve()  # run rocketry

@app.task("every 1 second", execution="async")
async def publish(br: NatsBroker = Arg("broker")):
    await br.publish("Hi, Rocketry!", "test")

if __name__ == "__main__":
    asyncio.run(start_app())
```
