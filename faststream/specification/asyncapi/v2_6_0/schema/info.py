from typing import (
    Optional,
    Union,
)

from pydantic import AnyHttpUrl

from faststream._internal.basic_types import AnyDict
from faststream.specification.asyncapi.v2_6_0.schema.contact import Contact
from faststream.specification.asyncapi.v2_6_0.schema.license import License
from faststream.specification.base.info import BaseApplicationInfo


class ApplicationInfo(BaseApplicationInfo):
    """A class to represent application information.

    Attributes:
        title : title of the information
        version : version of the information
        description : description of the information
        termsOfService : terms of service for the information
        contact : contact information for the information
        license : license information for the information
    """

    termsOfService: Optional[AnyHttpUrl] = None
    contact: Optional[Union[Contact, AnyDict]] = None
    license: Optional[Union[License, AnyDict]] = None
