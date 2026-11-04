from typing import Any

from pydantic import BaseModel

from faststream.specification.asyncapi.v2_6_0.schema import Reference


class Parameter(BaseModel):
    """A class to represent an AsyncAPI 3.0 channel parameter."""

    enum: list[Any] | None = None
    default: Any = None
    description: str | None = None
    examples: list[Any] | None = None


__all__ = (
    "Parameter",
    "Reference",
)
