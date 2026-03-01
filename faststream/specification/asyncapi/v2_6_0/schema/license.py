from typing import Any, cast, overload

from pydantic import AnyHttpUrl, BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream._internal.utils.data import filter_by_dict
from faststream.specification.schema.extra import (
    License as SpecLicense,
    LicenseDict,
)


class License(BaseModel):
    """A class to represent a license.

    Attributes:
        name : name of the license
        url : URL of the license (optional)

    Config:
        extra : allow additional attributes in the model (PYDANTIC_V2)
    """

    name: str
    # Use default values to be able build from dict
    url: AnyHttpUrl | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"

    @overload
    @classmethod
    def from_spec(cls, license: None) -> None: ...

    @overload
    @classmethod
    def from_spec(cls, license: SpecLicense) -> Self: ...

    @overload
    @classmethod
    def from_spec(cls, license: LicenseDict) -> Self: ...

    @overload
    @classmethod
    def from_spec(cls, license: dict[str, Any]) -> dict[str, Any]: ...

    @classmethod
    def from_spec(
        cls,
        license: SpecLicense | LicenseDict | dict[str, Any] | None,
    ) -> Self | dict[str, Any] | None:
        if license is None:
            return None

        if isinstance(license, SpecLicense):
            return cls(
                name=license.name,
                url=license.url,
            )

        license = cast("dict[str, Any]", license)
        license_data, custom_data = filter_by_dict(LicenseDict, license)

        if custom_data:
            return license

        return cls(**license_data)
