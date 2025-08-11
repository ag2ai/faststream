from typing import Any

from pydantic import BaseModel

from faststream._internal._compat import model_to_json, model_to_jsonable

from .info import BaseApplicationInfo


class BaseApplicationSchema(BaseModel):
    """A class to represent a Pydantic-serializable application schema.

    Attributes:
        info : information about the schema

    Methods:
        to_jsonable() -> Any: Convert the schema to a JSON-serializable object.
        to_json() -> str: Convert the schema to a JSON string.
        to_yaml() -> str: Convert the schema to a YAML string.
    """

    info: BaseApplicationInfo

    @property
    def title(self) -> str:
        return self.info.title

    def to_jsonable(self) -> Any:
        """Convert the schema to a JSON-serializable object."""
        return model_to_jsonable(
            self,
            by_alias=True,
            exclude_none=True,
        )

    def to_json(self) -> str:
        """Convert the schema to a JSON string."""
        return model_to_json(
            self,
            by_alias=True,
            exclude_none=True,
        )

    def to_yaml(self) -> str:
        """Convert the schema to a YAML string."""
        from io import StringIO

        import yaml

        io = StringIO(initial_value="", newline="\n")
        yaml.dump(self.to_jsonable(), io, sort_keys=False)
        return io.getvalue()
