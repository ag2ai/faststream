from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from faststream.specification.base.specification import Specification

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, AnyHttpUrl
    from faststream._internal.broker import BrokerUsecase
    from faststream.specification.schema import (
        Contact,
        ExternalDocs,
        License,
        Tag,
    )


def AsyncAPI(  # noqa: N802
    broker: "BrokerUsecase[Any, Any]",
    /,
    title: str = "FastStream",
    app_version: str = "0.1.0",
    schema_version: Literal["3.0.0", "2.6.0"] | str = "3.0.0",
    description: str = "",
    terms_of_service: Optional["AnyHttpUrl"] = None,
    license: Union["License", "AnyDict"] | None = None,
    contact: Union["Contact", "AnyDict"] | None = None,
    tags: Sequence[Union["Tag", "AnyDict"]] = (),
    external_docs: Union["ExternalDocs", "AnyDict"] | None = None,
    identifier: str | None = None,
) -> Specification:
    if schema_version.startswith("3.0."):
        from .v3_0_0.facade import AsyncAPI3

        return AsyncAPI3(
            broker,
            title=title,
            app_version=app_version,
            schema_version=schema_version,
            description=description,
            terms_of_service=terms_of_service,
            contact=contact,
            license=license,
            identifier=identifier,
            tags=tags,
            external_docs=external_docs,
        )

    if schema_version.startswith("2.6."):
        from .v2_6_0.facade import AsyncAPI2

        return AsyncAPI2(
            broker,
            title=title,
            app_version=app_version,
            schema_version=schema_version,
            description=description,
            terms_of_service=terms_of_service,
            contact=contact,
            license=license,
            identifier=identifier,
            tags=tags,
            external_docs=external_docs,
        )

    msg = f"Unsupported schema version: {schema_version}"
    raise NotImplementedError(msg)
