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
