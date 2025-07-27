from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .types import Receive, Scope, Send


class AsgiResponse:
    def __init__(
        self,
        body: bytes = b"",
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.body = body
        self.raw_headers = _get_response_headers(body, headers, status_code)

    def __repr__(self) -> str:
        inner = [f"status_code={self.status_code}"]
        if (ln := len(self.body)) > 100:
            inner.append(
                f"body={self.body[:100]!r}".rstrip(" \\n'") + f" ...' ({ln} bytes)"
            )
        else:
            inner.append(f"body={self.body!r}")
        inner.append(f"headers={self.raw_headers}")
        return f"{self.__class__.__name__}({', '.join(inner)})"

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        prefix = "websocket." if (scope["type"] == "websocket") else ""
        await send(
            {
                "type": f"{prefix}http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            },
        )
        await send(
            {
                "type": f"{prefix}http.response.body",
                "body": self.body,
            },
        )


def _get_response_headers(
    body: bytes,
    headers: Mapping[str, str] | None,
    status_code: int,
) -> list[tuple[bytes, bytes]]:
    if headers is None:
        raw_headers: list[tuple[bytes, bytes]] = []
        populate_content_length = True

    else:
        raw_headers = [
            (k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()
        ]
        keys = [h[0] for h in raw_headers]
        populate_content_length = b"content-length" not in keys

    if (
        body
        and populate_content_length
        and not (status_code < 200 or status_code in {204, 304})
    ):
        content_length = str(len(body))
        raw_headers.append((b"content-length", content_length.encode("latin-1")))

    return raw_headers
