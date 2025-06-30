from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Union

from faststream.specification.asyncapi.site import (
    ASYNCAPI_CSS_DEFAULT_URL,
    ASYNCAPI_JS_DEFAULT_URL,
    get_asyncapi_html,
)

from .handlers import get
from .response import AsgiResponse

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream._internal.broker import BrokerUsecase
    from faststream.specification import Specification
    from faststream.specification.schema import Tag, TagDict

    from .types import ASGIApp, Scope


def make_ping_asgi(
    broker: "BrokerUsecase[Any, Any]",
    /,
    timeout: float | None = None,
    include_in_schema: bool = True,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", "AnyDict"]] | None = None,
    unique_id: str | None = None,
) -> "ASGIApp":
    healthy_response = AsgiResponse(b"", 204)
    unhealthy_response = AsgiResponse(b"", 500)

    @get(
        include_in_schema=include_in_schema,
        description=description,
        tags=tags,
        unique_id=unique_id,
    )
    async def ping(scope: "Scope") -> AsgiResponse:
        if await broker.ping(timeout):
            return healthy_response
        return unhealthy_response

    return ping


def make_asyncapi_asgi(
    schema: "Specification",
    sidebar: bool = True,
    info: bool = True,
    servers: bool = True,
    operations: bool = True,
    messages: bool = True,
    schemas: bool = True,
    errors: bool = True,
    include_in_schema: bool = True,
    expand_message_examples: bool = True,
    asyncapi_js_url: str = ASYNCAPI_JS_DEFAULT_URL,
    asyncapi_css_url: str = ASYNCAPI_CSS_DEFAULT_URL,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", "AnyDict"]] | None = None,
    unique_id: str | None = None,
) -> "ASGIApp":
    cached_docs = None

    @get(
        include_in_schema=include_in_schema,
        description=description,
        tags=tags,
        unique_id=unique_id,
    )
    async def docs(scope: "Scope") -> AsgiResponse:
        nonlocal cached_docs
        if not cached_docs:
            cached_docs = get_asyncapi_html(
                schema.schema,
                sidebar=sidebar,
                info=info,
                servers=servers,
                operations=operations,
                messages=messages,
                schemas=schemas,
                errors=errors,
                expand_message_examples=expand_message_examples,
                asyncapi_js_url=asyncapi_js_url,
                asyncapi_css_url=asyncapi_css_url,
            )
        return AsgiResponse(
            cached_docs.encode("utf-8"),
            200,
            {"Content-Type": "text/html; charset=utf-8"},
        )

    return docs
