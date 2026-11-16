from fast_depends import Depends, Provider
from starlette.testclient import TestClient

from faststream.asgi import AsgiFastStream, AsgiResponse, get


class TestAsgiDependencyProvider:
    def test_custom_provider_override_is_used(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3350."""

        def dependency() -> str:
            return "original"

        def replacement() -> str:
            return "overridden"

        provider = Provider()
        provider.override(dependency, replacement)

        @get
        async def handler(value: str = Depends(dependency)) -> AsgiResponse:
            return AsgiResponse(value.encode())

        client = TestClient(
            AsgiFastStream(provider=provider, asgi_routes=[("/test", handler)])
        )
        assert client.get("/test").text == "overridden"

    def test_default_provider_still_calls_original_dependency(self) -> None:
        def dependency() -> str:
            return "original"

        @get
        async def handler(value: str = Depends(dependency)) -> AsgiResponse:
            return AsgiResponse(value.encode())

        client = TestClient(AsgiFastStream(asgi_routes=[("/test", handler)]))
        assert client.get("/test").text == "original"
