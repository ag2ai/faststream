from dataclasses import dataclass

from pydantic import AnyHttpUrl
from typing_extensions import Required, TypedDict


class LicenseDict(TypedDict, total=False):
    name: Required[str]
    url: AnyHttpUrl | str


@dataclass
class License:
    name: str
    url: AnyHttpUrl | str | None = None
