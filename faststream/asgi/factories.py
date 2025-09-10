from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Union

from faststream.specification.asyncapi.site import (
    ASYNCAPI_CSS_DEFAULT_URL,
    ASYNCAPI_JS_DEFAULT_URL,
    get_asyncapi_html,
)

from .handlers import get
from .response import AsgiResponse
from .types import ASGIApp, Scope

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase
    from faststream.specification.base import SpecificationFactory
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


class AsyncAPIRoute:
    def __init__(
        self,
        path: str,
        description: str | None = None,
        tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
        unique_id: str | None = None,
        include_in_schema: bool = False,
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
    ) -> None:
        self.path = path

        self.description = description
        self.tags = tags
        self.unique_id = unique_id
        self.include_in_schema = include_in_schema

        self.sidebar = sidebar
        self.info = info
        self.servers = servers
        self.operations = operations
        self.messages = messages
        self.schemas = schemas
        self.errors = errors
        self.expand_message_examples = expand_message_examples
        self.asyncapi_js_url = asyncapi_js_url
        self.asyncapi_css_url = asyncapi_css_url

    @classmethod
    def ensure_route(cls, path: Union[str, "AsyncAPIRoute"]) -> "AsyncAPIRoute":
        if isinstance(path, AsyncAPIRoute):
            return path
        return AsyncAPIRoute(path)

    def __call__(self, schema: "SpecificationFactory") -> "ASGIApp":
        return make_asyncapi_asgi(
            schema,
            description=self.description,
            tags=self.tags,
            unique_id=self.unique_id,
            include_in_schema=self.include_in_schema,
            sidebar=self.sidebar,
            info=self.info,
            servers=self.servers,
            operations=self.operations,
            messages=self.messages,
            schemas=self.schemas,
            errors=self.errors,
            expand_message_examples=self.expand_message_examples,
            asyncapi_js_url=self.asyncapi_js_url,
            asyncapi_css_url=self.asyncapi_css_url,
        )


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
) -> ASGIApp:
    cached_docs = None

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
            )
        return AsgiResponse(
            cached_docs.encode("utf-8"),
            200,
            {"Content-Type": "text/html; charset=utf-8"},
        )

    return docs
