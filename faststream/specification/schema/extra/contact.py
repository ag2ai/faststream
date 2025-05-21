from dataclasses import dataclass
from typing import Optional

from pydantic import AnyHttpUrl
from typing_extensions import Required, TypedDict

from faststream._internal._compat import EmailStr


class ContactDict(TypedDict, total=False):
    name: Required[str]
    url: AnyHttpUrl
    email: EmailStr


@dataclass
class Contact:
    name: str
    url: Optional[AnyHttpUrl] = None
    email: Optional[EmailStr] = None
