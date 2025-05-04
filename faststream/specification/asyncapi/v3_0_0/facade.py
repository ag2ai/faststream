from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream.specification.base.specification import Specification

from .generate import get_app_schema
from .schema import ApplicationSchema

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, AnyHttpUrl
    from faststream._internal.broker.broker import BrokerUsecase
    from faststream.specification.schema.extra import (
        Contact,
        ContactDict,
        ExternalDocs,
        ExternalDocsDict,
        License,
        LicenseDict,
        Tag,
        TagDict,
    )


class AsyncAPI3(Specification):
    def __init__(
        self,
        broker: "BrokerUsecase[Any, Any]",
        /,
        title: str = "FastStream",
        app_version: str = "0.1.0",
        schema_version: str = "3.0.0",
        description: str = "",
        terms_of_service: Optional["AnyHttpUrl"] = None,
        contact: Optional[Union["Contact", "ContactDict", "AnyDict"]] = None,
        license: Optional[Union["License", "LicenseDict", "AnyDict"]] = None,
        identifier: Optional[str] = None,
        tags: Sequence[Union["Tag", "TagDict", "AnyDict"]] = (),
        external_docs: Optional[
            Union["ExternalDocs", "ExternalDocsDict", "AnyDict"]
        ] = None,
    ) -> None:
        self.broker = broker
        self.title = title
        self.app_version = app_version
        self.schema_version = schema_version
        self.description = description
        self.terms_of_service = terms_of_service
        self.contact = contact
        self.license = license
        self.identifier = identifier
        self.tags = tags
        self.external_docs = external_docs

    def to_json(self) -> str:
        return self.schema.to_json()

    def to_jsonable(self) -> Any:
        return self.schema.to_jsonable()

    def to_yaml(self) -> str:
        return self.schema.to_yaml()

    @property
    def schema(self) -> ApplicationSchema:
        return get_app_schema(
            self.broker,
            title=self.title,
            app_version=self.app_version,
            schema_version=self.schema_version,
            description=self.description,
            terms_of_service=self.terms_of_service,
            contact=self.contact,
            license=self.license,
            identifier=self.identifier,
            tags=self.tags,
            external_docs=self.external_docs,
        )
