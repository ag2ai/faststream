import pytest
from starlette.testclient import TestClient

from faststream.asgi import AsgiFastStream


@pytest.mark.asyncio()
async def test_chained_exception_cause_preserved_on_startup_failure() -> None:
    """AsgiFastStream must not discard __cause__ when re-raising startup exceptions unwrapped from anyio's ExceptionGroup.

    This is a regression test for https://github.com/ag2ai/faststream/issues/2780
    """
    app = AsgiFastStream()

    cause = ConnectionError("underlying cause")

    @app.on_startup
    async def failing_startup() -> None:
        msg = "application startup failed"
        raise RuntimeError(msg) from cause

    with pytest.raises(RuntimeError) as exc_info, TestClient(app):
        pass

    assert exc_info.value.__cause__ is cause
