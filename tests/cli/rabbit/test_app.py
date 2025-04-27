import logging
import os
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, patch

import anyio
import pytest

from faststream import FastStream, TestApp
from faststream._internal._compat import IS_WINDOWS
from faststream._internal.log import logger
from faststream.rabbit import RabbitBroker, TestRabbitBroker


def test_init(app: FastStream, broker: RabbitBroker) -> None:
    assert app.broker is broker
    assert app.logger is logger


def test_init_without_broker(app_without_broker: FastStream) -> None:
    assert app_without_broker.broker is None


def test_init_without_logger(app_without_logger: FastStream) -> None:
    assert app_without_logger.logger is None


def test_set_broker(broker: RabbitBroker, app_without_broker: FastStream) -> None:
    assert app_without_broker.broker is None
    app_without_broker.set_broker(broker)
    assert app_without_broker.broker is broker


@pytest.mark.asyncio()
async def test_set_broker_in_on_startup_hook(
    app_without_broker: FastStream, broker: RabbitBroker
) -> None:
    def add_broker() -> None:
        app_without_broker.set_broker(broker)

    app_without_broker.on_startup(add_broker)

    async with TestRabbitBroker(broker):
        await app_without_broker._startup()


@pytest.mark.asyncio()
async def test_startup_fails_if_no_broker_was_provided(
    app_without_broker: FastStream,
) -> None:
    with pytest.raises(AssertionError):
        await app_without_broker._startup()


def test_log(app: FastStream, app_without_logger: FastStream) -> None:
    app._log(logging.INFO, "test")
    app_without_logger._log(logging.INFO, "test")


@pytest.mark.asyncio()
async def test_on_startup_calls(async_mock: AsyncMock, mock: Mock) -> None:
    def call1() -> None:
        mock.call_start1()
        assert not async_mock.call_start2.called

    async def call2() -> None:
        await async_mock.call_start2()
        assert mock.call_start1.call_count == 1

    test_app = FastStream(AsyncMock(), on_startup=[call1, call2])

    await test_app.start()

    mock.call_start1.assert_called_once()
    async_mock.call_start2.assert_called_once()


@pytest.mark.asyncio()
async def test_startup_calls_lifespans(
    mock: Mock,
    app: FastStream,
    async_mock: AsyncMock,
) -> None:
    def call1() -> None:
        mock.call_start1()
        assert not mock.call_start2.called

    def call2() -> None:
        mock.call_start2()
        assert mock.call_start1.call_count == 1

    app.on_startup(call1)
    app.on_startup(call2)

    with patch.object(app.broker, "start", async_mock):
        await app.start()

    mock.call_start1.assert_called_once()
    mock.call_start2.assert_called_once()


@pytest.mark.asyncio()
async def test_on_shutdown_calls(async_mock: AsyncMock, mock: Mock) -> None:
    def call1() -> None:
        mock.call_stop1()
        assert not async_mock.call_stop2.called

    async def call2() -> None:
        await async_mock.call_stop2()
        assert mock.call_stop1.call_count == 1

    test_app = FastStream(AsyncMock(), on_shutdown=[call1, call2])

    await test_app.stop()

    mock.call_stop1.assert_called_once()
    async_mock.call_stop2.assert_called_once()


@pytest.mark.asyncio()
async def test_shutdown_calls_lifespans(mock: Mock) -> None:
    app = FastStream(AsyncMock())

    def call1() -> None:
        mock.call_stop1()
        assert not mock.call_stop2.called

    def call2() -> None:
        mock.call_stop2()
        assert mock.call_stop1.call_count == 1

    app.on_shutdown(call1)
    app.on_shutdown(call2)

    await app.stop()

    mock.call_stop1.assert_called_once()
    mock.call_stop2.assert_called_once()


@pytest.mark.asyncio()
async def test_after_startup_calls(
    async_mock: AsyncMock,
    mock: Mock,
    broker: RabbitBroker,
) -> None:
    def call1() -> None:
        mock.after_startup1()
        assert not async_mock.after_startup2.called

    async def call2() -> None:
        await async_mock.after_startup2()
        assert mock.after_startup1.call_count == 1

    test_app = FastStream(broker, after_startup=[call1, call2])

    with patch.object(test_app.broker, "start", async_mock.broker_start):
        await test_app.start()

    mock.after_startup1.assert_called_once()
    async_mock.after_startup2.assert_called_once()


@pytest.mark.asyncio()
async def test_startup_lifespan_before_broker_started(
    async_mock: AsyncMock,
    app: FastStream,
) -> None:
    @app.on_startup
    async def call() -> None:
        await async_mock.before()
        assert not async_mock.broker_start.called

    @app.after_startup
    async def call_after() -> None:
        await async_mock.after()
        async_mock.before.assert_awaited_once()
        async_mock.broker_start.assert_called_once()

    with (
        patch.object(app.broker, "start", async_mock.broker_start),
        patch.object(
            app.broker,
            "connect",
            async_mock.broker_connect,
        ),
    ):
        await app.start()

    async_mock.broker_start.assert_called_once()
    async_mock.after.assert_awaited_once()
    async_mock.before.assert_awaited_once()


@pytest.mark.asyncio()
async def test_after_shutdown_calls(
    async_mock: AsyncMock,
    mock: Mock,
    broker: RabbitBroker,
) -> None:
    def call1() -> None:
        mock.after_shutdown1()
        assert not async_mock.after_shutdown2.called

    async def call2() -> None:
        await async_mock.after_shutdown2()
        assert mock.after_shutdown1.call_count == 1

    test_app = FastStream(broker, after_shutdown=[call1, call2])

    with (
        patch.object(test_app.broker, "start", async_mock.broker_start),
        patch.object(
            test_app.broker,
            "connect",
            async_mock.broker_connect,
        ),
    ):
        await test_app.stop()

    mock.after_shutdown1.assert_called_once()
    async_mock.after_shutdown2.assert_called_once()


@pytest.mark.asyncio()
async def test_shutdown_lifespan_after_broker_stopped(
    async_mock: AsyncMock,
    app: FastStream,
) -> None:
    @app.after_shutdown
    async def call() -> None:
        await async_mock.after()
        async_mock.broker_stop.assert_called_once()

    @app.on_shutdown
    async def call_before() -> None:
        await async_mock.before()
        assert not async_mock.broker_stop.called

    with patch.object(app.broker, "close", async_mock.broker_stop):
        await app.stop()

    async_mock.broker_stop.assert_called_once()
    async_mock.after.assert_awaited_once()
    async_mock.before.assert_awaited_once()


@pytest.mark.asyncio()
async def test_running(async_mock: AsyncMock, app: FastStream) -> None:
    app.exit()

    with (
        patch.object(app.broker, "start", async_mock.broker_run),
        patch.object(app.broker, "close", async_mock.broker_stopped),
    ):
        await app.run()

    async_mock.broker_run.assert_called_once()
    async_mock.broker_stopped.assert_called_once()


@pytest.mark.asyncio()
async def test_exception_group(async_mock: AsyncMock, app: FastStream) -> None:
    async_mock.side_effect = ValueError("Ooops!")

    @app.on_startup
    async def f() -> None:
        await async_mock()

    with pytest.raises(ValueError, match="Ooops!"):
        await app.run()


@pytest.mark.asyncio()
async def test_running_lifespan_contextmanager(
    async_mock: AsyncMock,
    mock: Mock,
    app: FastStream,
) -> None:
    @asynccontextmanager
    async def lifespan(env: str) -> AsyncIterator[None]:
        mock.on(env)
        yield
        mock.off()

    app = FastStream(async_mock, lifespan=lifespan)
    app.exit()

    await app.run(run_extra_options={"env": "test"})

    async_mock.start.assert_called_once()
    async_mock.close.assert_called_once()

    mock.on.assert_called_once_with("test")
    mock.off.assert_called_once()


@pytest.mark.asyncio()
async def test_test_app(mock: Mock) -> None:
    app = FastStream(AsyncMock())

    app.on_startup(mock.on)
    app.on_shutdown(mock.off)

    async with TestApp(app):
        pass

    mock.on.assert_called_once()
    mock.off.assert_called_once()


@pytest.mark.asyncio()
async def test_test_app_with_excp(mock: Mock) -> None:
    app = FastStream(AsyncMock())

    app.on_startup(mock.on)
    app.on_shutdown(mock.off)

    with pytest.raises(ValueError):  # noqa: PT011
        async with TestApp(app):
            raise ValueError

    mock.on.assert_called_once()
    mock.off.assert_called_once()


def test_sync_test_app(mock: Mock) -> None:
    app = FastStream(AsyncMock())

    app.on_startup(mock.on)
    app.on_shutdown(mock.off)

    with TestApp(app):
        pass

    mock.on.assert_called_once()
    mock.off.assert_called_once()


def test_sync_test_app_with_excp(mock: Mock) -> None:
    app = FastStream(AsyncMock())

    app.on_startup(mock.on)
    app.on_shutdown(mock.off)

    with pytest.raises(ValueError), TestApp(app):  # noqa: PT011
        raise ValueError

    mock.on.assert_called_once()
    mock.off.assert_called_once()


@pytest.mark.asyncio()
async def test_lifespan_contextmanager(async_mock: AsyncMock, app: FastStream) -> None:
    @asynccontextmanager
    async def lifespan(env: str) -> AsyncIterator[None]:
        await async_mock.on(env)
        yield
        await async_mock.off()

    app = FastStream(app.broker, lifespan=lifespan)

    with (
        patch.object(app.broker, "start", async_mock.broker_run),
        patch.object(app.broker, "close", async_mock.broker_stopped),
    ):
        async with TestApp(app, {"env": "test"}):
            pass

    async_mock.on.assert_awaited_once_with("test")
    async_mock.off.assert_awaited_once()
    async_mock.broker_run.assert_called_once()
    async_mock.broker_stopped.assert_called_once()


def test_sync_lifespan_contextmanager(async_mock: AsyncMock, app: FastStream) -> None:
    @asynccontextmanager
    async def lifespan(env: str) -> AsyncIterator[None]:
        await async_mock.on(env)
        yield
        await async_mock.off()

    app = FastStream(app.broker, lifespan=lifespan)

    with (
        patch.object(app.broker, "start", async_mock.broker_run),
        patch.object(
            app.broker,
            "close",
            async_mock.broker_stopped,
        ),
        TestApp(
            app,
            {"env": "test"},
        ),
    ):
        pass

    async_mock.on.assert_awaited_once_with("test")
    async_mock.off.assert_awaited_once()
    async_mock.broker_run.assert_called_once()
    async_mock.broker_stopped.assert_called_once()


@pytest.mark.asyncio()
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
async def test_stop_with_sigint(async_mock: AsyncMock, app: FastStream) -> None:
    with (
        patch.object(app.broker, "start", async_mock.broker_run_sigint),
        patch.object(app.broker, "close", async_mock.broker_stopped_sigint),
    ):
        async with anyio.create_task_group() as tg:
            tg.start_soon(app.run)
            tg.start_soon(_kill, signal.SIGINT)

    async_mock.broker_run_sigint.assert_called_once()
    async_mock.broker_stopped_sigint.assert_called_once()


@pytest.mark.asyncio()
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
async def test_stop_with_sigterm(async_mock: AsyncMock, app: FastStream) -> None:
    with (
        patch.object(app.broker, "start", async_mock.broker_run_sigterm),
        patch.object(app.broker, "close", async_mock.broker_stopped_sigterm),
    ):
        async with anyio.create_task_group() as tg:
            tg.start_soon(app.run)
            tg.start_soon(_kill, signal.SIGTERM)

    async_mock.broker_run_sigterm.assert_called_once()
    async_mock.broker_stopped_sigterm.assert_called_once()


@pytest.mark.asyncio()
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
async def test_run_asgi(async_mock: AsyncMock, app: FastStream) -> None:
    asgi_routes = [("/", lambda scope, receive, send: None)]
    asgi_app = app.as_asgi(asgi_routes=asgi_routes)
    assert asgi_app.broker is app.broker
    assert asgi_app.logger is app.logger
    assert asgi_app.lifespan_context is app.lifespan_context
    assert asgi_app._on_startup_calling is app._on_startup_calling
    assert asgi_app._after_startup_calling is app._after_startup_calling
    assert asgi_app._on_shutdown_calling is app._on_shutdown_calling
    assert asgi_app._after_shutdown_calling is app._after_shutdown_calling
    assert asgi_app.routes == asgi_routes

    with (
        patch.object(app.broker, "start", async_mock.broker_run),
        patch.object(app.broker, "close", async_mock.broker_stopped),
    ):
        async with anyio.create_task_group() as tg:
            tg.start_soon(app.run)
            tg.start_soon(_kill, signal.SIGINT)

    async_mock.broker_run.assert_called_once()
    async_mock.broker_stopped.assert_called_once()


async def _kill(sig: int) -> None:
    os.kill(os.getpid(), sig)
