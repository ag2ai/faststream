from dataclasses import dataclass
from typing import Required

from pydantic import AnyHttpUrl
from typing_extensions import TypedDict

from faststream._internal._compat import EmailStr


class ContactDict(TypedDict, total=False):
    name: Required[str]
    url: AnyHttpUrl | str
    email: EmailStr


@dataclass(slots=True)
class Contact:
    name: str
    url: AnyHttpUrl | str | None = None
    email: EmailStr | None = None
