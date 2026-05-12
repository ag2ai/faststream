from typing import Any, cast, overload

from pydantic import AnyHttpUrl, BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream._internal.utils.data import filter_by_dict
from faststream.specification.schema.extra import (
    ExternalDocs as SpecDocs,
    ExternalDocsDict,
)


class ExternalDocs(BaseModel):
    """A class to represent external documentation.

    Attributes:
        url : URL of the external documentation
        description : optional description of the external documentation
    """

    url: AnyHttpUrl
    # Use default values to be able build from dict
    description: str | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"

    @overload
    @classmethod
    def from_spec(cls, docs: None) -> None: ...

    @overload
    @classmethod
    def from_spec(cls, docs: SpecDocs) -> Self: ...

    @overload
    @classmethod
    def from_spec(cls, docs: ExternalDocsDict) -> Self: ...

    @overload
    @classmethod
    def from_spec(cls, docs: dict[str, Any]) -> dict[str, Any]: ...

    @classmethod
    def from_spec(
        cls,
        docs: SpecDocs | ExternalDocsDict | dict[str, Any] | None,
    ) -> Self | dict[str, Any] | None:
        if docs is None:
            return None

        if isinstance(docs, SpecDocs):
            return cls(url=docs.url, description=docs.description)

        docs = cast("dict[str, Any]", docs)
        docs_data, custom_data = filter_by_dict(ExternalDocsDict, docs)

        if custom_data:
            return docs

        return cls(**docs_data)
