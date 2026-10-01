from io import StringIO
from typing import Any

from pydantic import BaseModel
from pydantic_core import to_jsonable_python

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
        return to_jsonable_python(
            self,
            by_alias=True,
            exclude_none=True,
        )

    def to_json(self) -> str:
        """Convert the schema to a JSON string."""
        return self.model_dump_json(
            by_alias=True,
            exclude_none=True,
        )

    def to_yaml(self) -> str:
        """Convert the schema to a YAML string."""
        import yaml

        io = StringIO(initial_value="", newline="\n")
        yaml.dump(self.to_jsonable(), io, sort_keys=False)
        return io.getvalue()
