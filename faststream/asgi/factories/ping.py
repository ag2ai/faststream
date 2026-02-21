from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Union

from faststream.asgi.handlers import get
from faststream.asgi.response import AsgiResponse
from faststream.asgi.types import ASGIApp, Scope

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase
    from faststream.specification.schema import Tag, TagDict


def make_ping_asgi(
    broker: "BrokerUsecase[Any, Any]",
    /,
    timeout: float | None = None,
    include_in_schema: bool = True,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
) -> ASGIApp:
    """Create healthcheck ASGI handler for the given broker."""
    healthy_response = AsgiResponse(b"", 204)
    unhealthy_response = AsgiResponse(b"", 500)

    @get(
        include_in_schema=include_in_schema,
        description=description,
        tags=tags,
        unique_id=unique_id,
    )
    async def ping(scope: Scope) -> AsgiResponse:
        if await broker.ping(timeout):
            return healthy_response
        return unhealthy_response

    return ping
