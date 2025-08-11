import pytest

from tests.marks import require_aiopika


@pytest.mark.asyncio()
@require_aiopika
async def test_handle() -> None:
    from examples.e08_testing import test_handle as _test

    await _test()
