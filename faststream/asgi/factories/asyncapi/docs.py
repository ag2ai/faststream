from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Union

from faststream.asgi.handlers import GetHandler, get
from faststream.asgi.response import AsgiResponse
from faststream.asgi.types import Scope
from faststream.specification.asyncapi.site import (
    ASYNCAPI_CSS_DEFAULT_URL,
    ASYNCAPI_JS_DEFAULT_URL,
    get_asyncapi_html,
)

if TYPE_CHECKING:
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema import Tag, TagDict


def make_asyncapi_asgi(
    schema: "SpecificationFactory",
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
    include_in_schema: bool = True,
    *,
    sidebar: bool = True,
    info: bool = True,
    servers: bool = True,
    operations: bool = True,
    messages: bool = True,
    schemas: bool = True,
    errors: bool = True,
    expand_message_examples: bool = True,
    asyncapi_js_url: str = ASYNCAPI_JS_DEFAULT_URL,
    asyncapi_css_url: str = ASYNCAPI_CSS_DEFAULT_URL,
    try_it_out: bool = True,
    try_it_out_url: str = "asyncapi/try",
) -> "GetHandler":
    """Create AsyncAPI documentation ASGI handler."""
    cached_docs: str | None = None

    @get(
        include_in_schema=include_in_schema,
        description=description,
        tags=tags,
        unique_id=unique_id,
    )
    async def docs(scope: Scope) -> AsgiResponse:
        nonlocal cached_docs
        if not cached_docs:
            cached_docs = get_asyncapi_html(
                schema.to_specification(),
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
                try_it_out=try_it_out,
                try_it_out_url=try_it_out_url,
            )
        return AsgiResponse(
            cached_docs.encode("utf-8"),
            200,
            {"Content-Type": "text/html; charset=utf-8"},
        )

    return docs
