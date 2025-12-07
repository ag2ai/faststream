import json
import sys
import warnings
from collections import UserString
from collections.abc import Callable, Iterable, Mapping
from importlib.util import find_spec
from typing import (
    Any,
    TypeVar,
)

from pydantic import BaseModel
from pydantic.version import VERSION as PYDANTIC_VERSION

IS_WINDOWS = sys.platform in {"win32", "cygwin", "msys"}
IS_MACOS = sys.platform == "darwin"

__all__ = (
    "HAS_TYPER",
    "PYDANTIC_V2",
    "BaseModel",
    "CoreSchema",
    "EmailStr",
    "ExceptionGroup",
    "GetJsonSchemaHandler",
    "PydanticUndefined",
    "json_dumps",
    "json_loads",
    "with_info_plain_validator_function",
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


ModelVar = TypeVar("ModelVar", bound=BaseModel)

JsonSchemaValue = Mapping[str, Any]
major, minor, *_ = PYDANTIC_VERSION.split(".")
_PYDANTCI_MAJOR, _PYDANTIC_MINOR = int(major), int(minor)

PYDANTIC_V2 = _PYDANTCI_MAJOR >= 2

if PYDANTIC_V2:
    if _PYDANTIC_MINOR >= 4:
        from pydantic.annotated_handlers import (
            GetJsonSchemaHandler,
        )
        from pydantic_core.core_schema import (
            with_info_plain_validator_function,
        )
    else:
        from pydantic._internal._annotated_handlers import (  # type: ignore[no-redef]
            GetJsonSchemaHandler,
        )
        from pydantic_core.core_schema import (
            general_plain_validator_function as with_info_plain_validator_function,
        )

    from pydantic_core import CoreSchema, PydanticUndefined, to_jsonable_python

    SCHEMA_FIELD = "json_schema_extra"
    DEF_KEY = "$defs"

    def model_to_jsonable(
        model: BaseModel,
        **kwargs: Any,
    ) -> Any:
        return to_jsonable_python(model, **kwargs)

    def dump_json(data: Any) -> bytes:
        return json_dumps(model_to_jsonable(data))

    def get_model_fields(model: type[BaseModel]) -> dict[str, Any]:
        return model.__pydantic_fields__

    def model_to_json(model: BaseModel, **kwargs: Any) -> str:
        return model.model_dump_json(**kwargs)

    def model_parse(
        model: type[ModelVar],
        data: str | bytes,
        **kwargs: Any,
    ) -> ModelVar:
        return model.model_validate_json(data, **kwargs)

    def model_schema(model: type[BaseModel], **kwargs: Any) -> dict[str, Any]:
        return model.model_json_schema(**kwargs)

else:
    from pydantic.json import pydantic_encoder

    GetJsonSchemaHandler = Any  # type: ignore[assignment,misc]
    CoreSchema = Any  # type: ignore[assignment,misc]

    SCHEMA_FIELD = "schema_extra"
    DEF_KEY = "definitions"

    PydanticUndefined = Ellipsis  # type: ignore[assignment]

    def dump_json(data: Any) -> bytes:
        return json_dumps(data, default=pydantic_encoder)

    def get_model_fields(model: type[BaseModel]) -> dict[str, Any]:
        return model.__fields__  # type: ignore[return-value]

    def model_to_json(model: BaseModel, **kwargs: Any) -> str:
        return model.json(**kwargs)

    def model_parse(
        model: type[ModelVar],
        data: str | bytes,
        **kwargs: Any,
    ) -> ModelVar:
        return model.parse_raw(data, **kwargs)

    def model_schema(model: type[BaseModel], **kwargs: Any) -> dict[str, Any]:
        return model.schema(**kwargs)

    def model_to_jsonable(
        model: BaseModel,
        **kwargs: Any,
    ) -> Any:
        return json_loads(model.json(**kwargs))

    # TODO: pydantic types misc
    def with_info_plain_validator_function(  # type: ignore[misc]
        function: Callable[..., Any],
        *,
        ref: str | None = None,
        metadata: Any = None,
        serialization: Any = None,
    ) -> JsonSchemaValue:
        return {}


if sys.version_info >= (3, 11):
    ExceptionGroup = ExceptionGroup  # noqa: F821,PLW0127
else:
    from exceptiongroup import ExceptionGroup

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
                "email-validator bot installed, email fields will be treated as str.\n"
                "To install, run: pip install email-validator",
                category=RuntimeWarning,
                stacklevel=1,
            )
            return str(v)

        @classmethod
        def _validate(cls, __input_value: Any, _: Any) -> str:
            warnings.warn(
                "email-validator bot installed, email fields will be treated as str.\n"
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
