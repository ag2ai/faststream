from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Sequence,
    Union,
)

from faststream.asyncapi import get_app_schema
from faststream.asyncapi.site import (
    ASYNCAPI_CSS_DEFAULT_URL,
    ASYNCAPI_JS_DEFAULT_URL,
    get_asyncapi_html,
)

from .handlers import get
from .response import AsgiResponse

if TYPE_CHECKING:
    from faststream.asyncapi.proto import AsyncAPIApplication
    from faststream.asyncapi.schema import Tag, TagDict
    from faststream.broker.core.usecase import BrokerUsecase
    from faststream.types import AnyDict

    from .types import ASGIApp, Scope


def make_ping_asgi(
    broker: "BrokerUsecase[Any, Any]",
    /,
    timeout: Optional[float] = None,
    include_in_schema: bool = True,
    description: Optional[str] = None,
    tags: Optional[Sequence[Union["Tag", "TagDict", "AnyDict"]]] = None,
    unique_id: Optional[str] = None,
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
    app: "AsyncAPIApplication",
    sidebar: bool = True,
    info: bool = True,
    servers: bool = True,
    operations: bool = True,
    messages: bool = True,
    schemas: bool = True,
    errors: bool = True,
    expand_message_examples: bool = True,
    title: str = "FastStream",
    asyncapi_js_url: str = ASYNCAPI_JS_DEFAULT_URL,
    asyncapi_css_url: str = ASYNCAPI_CSS_DEFAULT_URL,
    include_in_schema: bool = True,
    description: Optional[str] = None,
    tags: Optional[Sequence[Union["Tag", "TagDict", "AnyDict"]]] = None,
    unique_id: Optional[str] = None,
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
                get_app_schema(app),
                sidebar=sidebar,
                info=info,
                servers=servers,
                operations=operations,
                messages=messages,
                schemas=schemas,
                errors=errors,
                expand_message_examples=expand_message_examples,
                title=title,
                asyncapi_js_url=asyncapi_js_url,
                asyncapi_css_url=asyncapi_css_url,
            )
        return AsgiResponse(
            cached_docs.encode("utf-8"),
            200,
            {"Content-Type": "text/html; charset=utf-8"},
        )

    return docs
