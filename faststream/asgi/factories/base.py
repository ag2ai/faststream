from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from faststream.asgi.types import ASGIApp
    from faststream.specification.base import SpecificationFactory


class AsyncAPIRouteFactory(Protocol):
    """Protocol for AsyncAPI docs route configuration."""

    path: str
    try_it_out: bool
    try_it_out_endpoint_base: str

    def __call__(self, schema: "SpecificationFactory") -> "ASGIApp": ...
