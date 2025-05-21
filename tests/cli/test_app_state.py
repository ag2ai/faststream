from unittest.mock import AsyncMock, patch

import pytest

from faststream import FastStream


@pytest.mark.asyncio()
async def test_state_running(app: FastStream) -> None:
    with patch(
        "faststream._internal.application.Application.start", new_callable=AsyncMock
    ):
        await app._startup()

        assert app.running


@pytest.mark.asyncio()
async def test_state_stopped(app: FastStream) -> None:
    with (
        patch(
            "faststream._internal.application.Application.start", new_callable=AsyncMock
        ),
        patch(
            "faststream._internal.application.Application.stop", new_callable=AsyncMock
        ),
    ):
        await app._startup()
        await app._shutdown()

        assert not app.running
