import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from faststream.rabbit import RabbitBroker
from tests.marks import require_aiopika

if TYPE_CHECKING:
    from aio_pika.abc import AbstractRobustConnection


@pytest.mark.parametrize("callback_type", (MagicMock, AsyncMock))
@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_close_callback_survives_broker_restart(
    settings: Any,
    callback_type: type[MagicMock],
) -> None:
    on_close = [callback_type(), callback_type()]
    on_reconnected = (callback_type(), callback_type())
    broker = RabbitBroker(settings.url, on_close=on_close, on_reconnected=on_reconnected)

    for _ in range(2):
        async with broker:
            connection = await broker.connect()
            assert await broker.connect() is connection
            for callback in (*on_close, *on_reconnected):
                callback.assert_not_called()

        for callback in on_close:
            callback.assert_called_once()
            assert callback.call_args.args[0] is connection
            if isinstance(callback, AsyncMock):
                callback.assert_awaited_once_with(*callback.call_args.args)
            callback.reset_mock()
        for callback in on_reconnected:
            callback.assert_not_called()


@pytest.mark.parametrize("callback_type", (MagicMock, AsyncMock))
@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_reconnected_callback_after_automatic_recovery(
    settings: Any,
    callback_type: type[MagicMock],
) -> None:
    recovered: asyncio.Queue[AbstractRobustConnection | None] = asyncio.Queue()
    on_close = (MagicMock(), MagicMock())
    on_reconnected = [
        callback_type(side_effect=recovered.put_nowait),
        callback_type(side_effect=recovered.put_nowait),
    ]
    broker = RabbitBroker(
        settings.url,
        reconnect_interval=0.01,
        on_close=on_close,
        on_reconnected=on_reconnected,
    )

    async with broker:
        connection = await broker.connect()
        assert connection.transport is not None
        await connection.transport.connection.close()
        for _ in on_reconnected:
            assert await asyncio.wait_for(recovered.get(), timeout=5) is connection
        for callback in on_reconnected:
            if isinstance(callback, AsyncMock):
                callback.assert_awaited_once_with(connection)
            else:
                callback.assert_called_once_with(connection)
        for callback in on_close:
            callback.assert_called_once()
            assert callback.call_args.args[0] is connection
        assert await broker.ping(timeout=1)
