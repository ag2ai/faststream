from dataclasses import dataclass
from typing import Required

from pydantic import AnyHttpUrl
from typing_extensions import TypedDict


class LicenseDict(TypedDict, total=False):
    name: Required[str]
    url: AnyHttpUrl | str


@dataclass
class License:
    name: str
    url: AnyHttpUrl | str | None = None
