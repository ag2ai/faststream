from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from faststream.specification.base import Specification, SpecificationFactory

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyHttpUrl
    from faststream._internal.broker import BrokerUsecase
    from faststream.asgi.handlers import HttpHandler
    from faststream.specification.schema import Contact, ExternalDocs, License, Tag


class AsyncAPI(SpecificationFactory):
    def __init__(
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
        title: str = "FastStream",
        version: str = "0.1.0",
        description: str | None = None,
        terms_of_service: Optional["AnyHttpUrl"] = None,
        license: Union["License", "dict[str, Any]"] | None = None,
        contact: Union["Contact", "dict[str, Any]"] | None = None,
        tags: Sequence[Union["Tag", "dict[str, Any]"]] = (),
        external_docs: Union["ExternalDocs", "dict[str, Any]"] | None = None,
        identifier: str | None = None,
        schema_version: Literal["3.0.0", "2.6.0"] | str = "3.0.0",
    ) -> None:
        self.title = title
        self.version = version
        self.description = description
        self.terms_of_service = terms_of_service
        self.license = license
        self.contact = contact
        self.tags = tags
        self.external_docs = external_docs
        self.identifier = identifier
        self.schema_version = schema_version

        self.brokers: list[BrokerUsecase[Any, Any]] = []
        if broker:
            self.add_broker(broker)

        self.http_handlers: list[tuple[str, HttpHandler]] = []

    def add_broker(
        self,
        broker: "BrokerUsecase[Any, Any]",
        /,
    ) -> "SpecificationFactory":
        if broker not in self.brokers:
            self.brokers.append(broker)
        return self

    def add_http_route(
        self,
        path: str,
        handler: "HttpHandler",
    ) -> "SpecificationFactory":
        self.http_handlers.append((path, handler))
        return self

    def to_specification(self) -> Specification:
        if self.schema_version.startswith("3."):
            from .v3_0_0 import get_app_schema as schema_3_0

            return schema_3_0(
                self.brokers[0],
                title=self.title,
                app_version=self.version,
                schema_version=self.schema_version,
                description=self.description,
                terms_of_service=self.terms_of_service,
                contact=self.contact,
                license=self.license,
                identifier=self.identifier,
                tags=self.tags,
                external_docs=self.external_docs,
                http_handlers=self.http_handlers,
            )

        if self.schema_version.startswith("2.6."):
            from .v2_6_0 import get_app_schema as schema_2_6

            return schema_2_6(
                self.brokers[0],
                title=self.title,
                app_version=self.version,
                schema_version=self.schema_version,
                description=self.description,
                terms_of_service=self.terms_of_service,
                contact=self.contact,
                license=self.license,
                identifier=self.identifier,
                tags=self.tags,
                external_docs=self.external_docs,
                http_handlers=self.http_handlers,
            )

        msg = f"Unsupported schema version: {self.schema_version}"
        raise NotImplementedError(msg)
