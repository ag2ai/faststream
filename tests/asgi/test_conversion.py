from typing import Annotated

from fast_depends import Provider
from starlette.testclient import TestClient

from faststream import FastStream
from faststream._internal.context import Context, ContextRepo
from faststream.asgi import AsgiResponse, get


class TestAsgiConversion:
    def test_dependency_provider_is_preserved(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3349."""
        provider = Provider()
        app = FastStream(provider=provider)
        assert app.as_asgi().config.provider is provider

    def test_custom_context_is_preserved(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3349."""
        context = ContextRepo()
        context.set_global("custom_value", "configured")

        @get
        async def handler(value: Annotated[str, Context("custom_value")]) -> AsgiResponse:
            return AsgiResponse(value.encode())

        app = FastStream(context=context)
        client = TestClient(app.as_asgi(asgi_routes=[("/test", handler)]))
        assert client.get("/test").text == "configured"

    def test_disabled_serializer_is_preserved(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3349."""
        app = FastStream(serializer=None)
        assert app.as_asgi().config.serializer is None
