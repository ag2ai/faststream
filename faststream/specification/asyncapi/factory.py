from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from faststream.specification.base import Specification, SpecificationFactory

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, AnyHttpUrl
    from faststream._internal.broker import BrokerUsecase
    from faststream.asgi.handlers import HttpHandler
    from faststream.specification.schema import (
        Contact,
        ExternalDocs,
        License,
        Tag,
    )


@dataclass
class AsyncAPI(SpecificationFactory):
    title: str = "FastStream"
    version: str = "0.1.0"
    description: str | None = None
    terms_of_service: Optional["AnyHttpUrl"] = None
    license: Union["License", "AnyDict"] | None = None
    contact: Union["Contact", "AnyDict"] | None = None
    tags: Sequence[Union["Tag", "AnyDict"]] = ()
    external_docs: Union["ExternalDocs", "AnyDict"] | None = None
    identifier: str | None = None

    schema_version: Literal["3.0.0", "2.6.0"] | str = "3.0.0"

    brokers: list["BrokerUsecase[Any, Any]"] = field(default_factory=list, init=False)
    http_handlers: list[tuple[str, "HttpHandler"]] = field(
        default_factory=list, init=False
    )

    def add_broker(
        self, broker: "BrokerUsecase[Any, Any]", /
    ) -> "SpecificationFactory":
        self.brokers.append(broker)
        return self

    def add_http_route(
        self, path: str, handler: "HttpHandler"
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
