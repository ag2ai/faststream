"""AsyncAPI related functions."""

from faststream.specification.asyncapi.site import get_asyncapi_html

from .factory import AsyncAPI

__all__ = (
    "AsyncAPI",
    "get_asyncapi_html",
)
