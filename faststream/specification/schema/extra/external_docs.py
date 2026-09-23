from dataclasses import dataclass
from typing import Required

from typing_extensions import TypedDict


class ExternalDocsDict(TypedDict, total=False):
    url: Required[str]
    description: str


@dataclass
class ExternalDocs:
    url: str
    description: str | None = None
