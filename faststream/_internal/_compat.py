import json
import sys
import warnings
from collections import UserString
from collections.abc import Callable, Iterable, Mapping
from importlib.util import find_spec
from typing import Any

from pydantic import BaseModel
from pydantic.annotated_handlers import GetJsonSchemaHandler
from pydantic_core import CoreSchema, to_jsonable_python
from pydantic_core.core_schema import with_info_plain_validator_function

IS_WINDOWS = sys.platform in {"win32", "cygwin", "msys"}
IS_MACOS = sys.platform == "darwin"

__all__ = (
    "HAS_TYPER",
    "BaseModel",
    "EmailStr",
    "dump_json",
    "json_dumps",
    "json_loads",
)

try:
    HAS_TYPER = find_spec("typer") is not None
except ImportError:
    HAS_TYPER = False


json_dumps: Callable[..., bytes]
orjson: Any

try:
    import orjson  # type: ignore[no-redef]
except ImportError:
    orjson = None

if orjson:
    json_loads = orjson.loads
    json_dumps = orjson.dumps
else:
    json_loads = json.loads

    def json_dumps(*a: Any, **kw: Any) -> bytes:
        return json.dumps(*a, **kw).encode()


JsonSchemaValue = Mapping[str, Any]


def dump_json(data: Any) -> bytes:
    return json_dumps(to_jsonable_python(data))


try:
    import email_validator

    if email_validator is None:
        raise ImportError
    from pydantic import EmailStr
except ImportError:  # pragma: no cover
    # NOTE: EmailStr mock was copied from the FastAPI
    # https://github.com/tiangolo/fastapi/blob/master/fastapi/openapi/models.py#24
    class EmailStr(UserString):  # type: ignore[no-redef]
        """EmailStr is a string that should be an email.

        Note: EmailStr mock was copied from the FastAPI:
        https://github.com/tiangolo/fastapi/blob/master/fastapi/openapi/models.py#24
        """

        @classmethod
        def __get_validators__(cls) -> Iterable[Callable[..., Any]]:
            """Returns the validators for the EmailStr class."""
            yield cls.validate

        @classmethod
        def validate(cls, v: Any) -> str:
            """Validates the EmailStr class."""
            warnings.warn(
                "email-validator not installed, email fields will be treated as str.\n"
                "To install, run: pip install email-validator",
                category=RuntimeWarning,
                stacklevel=1,
            )
            return str(v)

        @classmethod
        def _validate(cls, __input_value: Any, _: Any) -> str:
            warnings.warn(
                "email-validator not installed, email fields will be treated as str.\n"
                "To install, run: pip install email-validator",
                category=RuntimeWarning,
                stacklevel=1,
            )
            return str(__input_value)

        @classmethod
        def __get_pydantic_json_schema__(
            cls,
            core_schema: CoreSchema,
            handler: GetJsonSchemaHandler,
        ) -> JsonSchemaValue:
            """Returns the JSON schema for the EmailStr class.

            Args:
                core_schema : the core schema
                handler : the handler
            """
            return {"type": "string", "format": "email"}

        @classmethod
        def __get_pydantic_core_schema__(
            cls,
            source: type[Any],
            handler: Callable[[Any], CoreSchema],
        ) -> JsonSchemaValue:
            """Returns the core schema for the EmailStr class.

            Args:
                source : the source
                handler : the handler
            """
            return with_info_plain_validator_function(cls._validate)


uvicorn: Any

try:
    import uvicorn

    HAS_UVICORN = True

except ImportError:
    uvicorn = None
    HAS_UVICORN = False

opentelemetry: Any

try:
    import opentelemetry
except ImportError:
    opentelemetry = None
    HAS_OPENTELEMETRY = False
else:
    HAS_OPENTELEMETRY = True
