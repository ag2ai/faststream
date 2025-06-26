from dataclasses import dataclass

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
    url: AnyHttpUrl | None = None
    email: EmailStr | None = None
