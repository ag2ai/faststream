from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch

import anyio
import pytest

from faststream import FastStream
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)


@asynccontextmanager
async def running_app(
    main: Callable[[], Coroutine[Any, Any, Any]],
) -> AsyncGenerator[Any, None]:
    """Run the snippet's `main()` and yield the app its blocking `run()` started."""
    started = anyio.Event()
    apps: list[Any] = []
    start = FastStream.start

    async def patched_start(self: FastStream, **run_extra_options: Any) -> None:
        await start(self, **run_extra_options)
        apps.append(self)
        started.set()

    with patch.object(FastStream, "start", patched_start):
        async with anyio.create_task_group() as tg:
            _ = tg.start_soon(main)

            with anyio.fail_after(10.0):
                await started.wait()

            app = apps[0]
            try:
                yield app
            finally:
                app.exit()


@pytest.mark.connected()
@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_manual_run() -> None:
    from docs.docs_src.getting_started.manual_run.kafka_base_run import main

    async with running_app(main) as app:
        assert await app.brokers[0].ping(timeout=5.0)


@pytest.mark.connected()
@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_confluent_manual_run() -> None:
    from docs.docs_src.getting_started.manual_run.confluent_base_run import main

    async with running_app(main) as app:
        assert await app.brokers[0].ping(timeout=5.0)


@pytest.mark.connected()
@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_manual_run() -> None:
    from docs.docs_src.getting_started.manual_run.rabbit_base_run import main

    async with running_app(main) as app:
        assert await app.brokers[0].ping(timeout=5.0)


@pytest.mark.connected()
@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_nats_manual_run() -> None:
    from docs.docs_src.getting_started.manual_run.nats_base_run import main

    async with running_app(main) as app:
        assert await app.brokers[0].ping(timeout=5.0)


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_redis_manual_run() -> None:
    from docs.docs_src.getting_started.manual_run.redis_base_run import main

    async with running_app(main) as app:
        assert await app.brokers[0].ping(timeout=5.0)


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_mqtt_manual_run() -> None:
    from docs.docs_src.getting_started.manual_run.mqtt_base_run import main

    async with running_app(main) as app:
        assert await app.brokers[0].ping(timeout=5.0)
