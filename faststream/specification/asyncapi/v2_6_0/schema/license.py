from typing import Any, Self, cast, overload

from pydantic import AnyHttpUrl, BaseModel

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
        extra : allow additional attributes in the model
    """

    name: str
    # Use default values to be able build from dict
    url: AnyHttpUrl | None = None

    model_config = {"extra": "allow"}

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
