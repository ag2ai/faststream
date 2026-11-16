from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from faststream.asgi import AsgiFastStream, AsgiResponse, get, post


class TestPostMethods:
    @pytest.mark.parametrize(
        "method",
        (
            pytest.param("HEAD", id="head"),
            pytest.param("GET", id="get"),
            pytest.param("PUT", id="put"),
        ),
    )
    def test_disallowed_methods_do_not_call_handler(
        self, method: str, mock: MagicMock
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3348."""

        @post
        async def handler() -> AsgiResponse:
            mock()
            return AsgiResponse(b"created")

        client = TestClient(AsgiFastStream(asgi_routes=[("/test", handler)]))
        response = client.request(method, "/test")
        assert response.status_code == 405
        assert response.headers["allow"] == "POST"
        mock.assert_not_called()

        assert client.post("/test").status_code == 200
        mock.assert_called_once_with()

    def test_get_still_allows_head(self) -> None:
        @get
        async def handler() -> AsgiResponse:
            return AsgiResponse(b"ok")

        client = TestClient(AsgiFastStream(asgi_routes=[("/test", handler)]))
        assert client.head("/test").status_code == 200
