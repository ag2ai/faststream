from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Union

from faststream.asgi.factories.asyncapi.docs import make_asyncapi_asgi
from faststream.specification.asyncapi.site import (
    ASYNCAPI_CSS_DEFAULT_URL,
    ASYNCAPI_JS_DEFAULT_URL,
    ASYNCAPI_TRY_IT_PLUGIN_URL,
)

if TYPE_CHECKING:
    from faststream.asgi.handlers import GetHandler
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema import Tag, TagDict


class AsyncAPIRoute:
    """Configuration for AsyncAPI documentation route with try-it-out support."""

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
        try_it_out_plugin_url: str = ASYNCAPI_TRY_IT_PLUGIN_URL,
        try_it_out: bool = True,
        try_it_out_url: str | None = None,
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
        self.try_it_out_plugin_url = try_it_out_plugin_url
        self.try_it_out = try_it_out
        if not try_it_out_url:
            try_it_out_url = path.rstrip("/") + "/try"
        self.try_it_out_url = try_it_out_url

    @classmethod
    def ensure_route(cls, path: Union[str, "AsyncAPIRoute"]) -> "AsyncAPIRoute":
        if isinstance(path, AsyncAPIRoute):
            return path
        return AsyncAPIRoute(path)

    def __call__(self, schema: "SpecificationFactory") -> "GetHandler":
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
            try_it_out_plugin_url=self.try_it_out_plugin_url,
            try_it_out=self.try_it_out,
            try_it_out_url=self.try_it_out_url,
        )
