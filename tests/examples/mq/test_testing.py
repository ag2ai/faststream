import pytest


@pytest.mark.mq()
@pytest.mark.asyncio()
async def test_testing() -> None:
    from examples.mq.testing import test_handle

    await test_handle()
