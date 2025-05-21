from dataclasses import dataclass
from typing import Optional

from pydantic import AnyHttpUrl
from typing_extensions import Required, TypedDict


class LicenseDict(TypedDict, total=False):
    name: Required[str]
    url: AnyHttpUrl


@dataclass
class License:
    name: str
    url: Optional[AnyHttpUrl] = None
