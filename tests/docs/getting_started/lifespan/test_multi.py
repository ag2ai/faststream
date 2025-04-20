import pytest

from faststream import TestApp


@pytest.mark.asyncio()
async def test_multi_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.multiple import app

    async with TestApp(app):
        assert app.context.get("field") == 1
