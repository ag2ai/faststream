from typing import (
    TYPE_CHECKING,
    Any,
)

from faststream.asgi.handlers import get
from faststream.asgi.response import AsgiResponse
from faststream.specification.asyncapi.site import (
    ASYNCAPI_CSS_DEFAULT_URL,
    ASYNCAPI_JS_DEFAULT_URL,
    get_asyncapi_html,
)

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase
    from faststream.asgi.types import ASGIApp, Scope
    from faststream.specification.base.specification import Specification


def make_ping_asgi(
    broker: "BrokerUsecase[Any, Any]",
    /,
    include_in_schema: bool = True,
    timeout: float | None = None,
) -> "ASGIApp":
    healthy_response = AsgiResponse(b"", 204)
    unhealthy_response = AsgiResponse(b"", 500)

    @get(include_in_schema=include_in_schema)
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
) -> "ASGIApp":
    cached_docs = None

    @get(include_in_schema=include_in_schema)
    async def docs(scope: "Scope") -> AsgiResponse:
        nonlocal cached_docs
        if not cached_docs:
            cached_docs = get_asyncapi_html(
                schema,
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
