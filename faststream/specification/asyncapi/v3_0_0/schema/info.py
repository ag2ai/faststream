from typing import (
    Union,
)

from pydantic import AnyHttpUrl

from faststream._internal.basic_types import (
    AnyDict,
)
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
    contact: Contact | AnyDict | None = None
    license: License | AnyDict | None = None
    tags: list[Union["Tag", "AnyDict"]] | None = None
    externalDocs: Union["ExternalDocs", "AnyDict"] | None = None
