from typing import (
    Optional,
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

    termsOfService: Optional[AnyHttpUrl] = None
    contact: Optional[Union[Contact, AnyDict]] = None
    license: Optional[Union[License, AnyDict]] = None
    tags: Optional[list[Union["Tag", "AnyDict"]]] = None
    externalDocs: Optional[Union["ExternalDocs", "AnyDict"]] = None
