from dataclasses import dataclass

from typing_extensions import Required, TypedDict


class ExternalDocsDict(TypedDict, total=False):
    url: Required[str]
    description: str


@dataclass
class ExternalDocs:
    url: str
    description: str | None = None
