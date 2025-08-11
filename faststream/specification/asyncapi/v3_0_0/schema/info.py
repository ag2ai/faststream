from typing import Any

from pydantic import AnyHttpUrl

from faststream.specification.asyncapi.v2_6_0.schema import (
    Contact,
    ExternalDocs,
    License,
    Tag,
)
from faststream.specification.base.info import BaseApplicationInfo


class ApplicationInfo(BaseApplicationInfo):
    """A class to represent application information.

    Attributes:
        termsOfService : terms of service for the information
        contact : contact information for the information
        license : license information for the information
        tags : optional list of tags
        externalDocs : optional external documentation
    """

    termsOfService: AnyHttpUrl | None = None
    contact: Contact | dict[str, Any] | None = None
    license: License | dict[str, Any] | None = None
    tags: list[Tag | dict[str, Any]] | None = None
    externalDocs: ExternalDocs | dict[str, Any] | None = None
