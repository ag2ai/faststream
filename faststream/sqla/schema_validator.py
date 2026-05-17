from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import Enum, Table, inspect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import (
    BINARY,
    BLOB,
    JSON,
    TIMESTAMP,
    VARBINARY,
    VARCHAR,
    BigInteger,
    DateTime,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    Text,
    TypeDecorator,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection


_INTEGER_TYPES: tuple[type[Any], ...] = (BigInteger, SmallInteger, Integer)
_STRING_TYPES: tuple[type[Any], ...] = (String, Text, VARCHAR)
_DATETIME_TYPES: tuple[type[Any], ...] = (DateTime, TIMESTAMP)
_BINARY_TYPES: tuple[type[Any], ...] = (LargeBinary, BLOB, BINARY, VARBINARY)
_JSON_TYPES: tuple[type[Any], ...] = (JSON, JSONB)


class SchemaValidator:
    _ALLOWED_TYPES_BY_COLUMN: ClassVar[dict[str, tuple[type[Any], ...]]] = {
        "id": _INTEGER_TYPES,
        "queue": _STRING_TYPES,
        "headers": _JSON_TYPES,
        "payload": _BINARY_TYPES,
        "state": (Enum,),
        "attempts_count": _INTEGER_TYPES,
        "deliveries_count": _INTEGER_TYPES,
        "created_at": _DATETIME_TYPES,
        "first_attempt_at": _DATETIME_TYPES,
        "next_attempt_at": _DATETIME_TYPES,
        "last_attempt_at": _DATETIME_TYPES,
        "acquired_at": _DATETIME_TYPES,
        "archived_at": _DATETIME_TYPES,
    }
    _ALLOWED_TYPE_NAMES_BY_COLUMN: ClassVar[dict[str, tuple[str, ...]]] = {
        "id": ("BigInteger", "Integer", "SmallInteger"),
        "queue": ("String", "Text", "VARCHAR"),
        "headers": ("JSON", "JSONB"),
        "payload": ("LargeBinary", "Binary", "BLOB", "VARBINARY"),
        "state": ("Enum",),
        "attempts_count": ("BigInteger", "Integer", "SmallInteger"),
        "deliveries_count": ("BigInteger", "Integer", "SmallInteger"),
        "created_at": ("DateTime", "TIMESTAMP"),
        "first_attempt_at": ("DateTime", "TIMESTAMP"),
        "next_attempt_at": ("DateTime", "TIMESTAMP"),
        "last_attempt_at": ("DateTime", "TIMESTAMP"),
        "acquired_at": ("DateTime", "TIMESTAMP"),
        "archived_at": ("DateTime", "TIMESTAMP"),
    }

    def __init__(
        self,
        *,
        message_table: Table,
        message_archive_table: Table,
    ) -> None:
        self._tables = (message_table, message_archive_table)

    def __call__(self, connection: Connection) -> list[str]:
        insp = inspect(connection)
        errors: list[str] = []

        for table_def in self._tables:
            table_name = table_def.name
            if not insp.has_table(table_name):
                errors.append(f"Table '{table_name}' does not exist")
                continue

            db_columns = {c["name"]: c["type"] for c in insp.get_columns(table_name)}
            expected_columns = {c.name: c.type for c in table_def.columns}

            missing = set(expected_columns.keys()) - set(db_columns.keys())
            if missing:
                errors.append(f"Table '{table_name}' missing columns: {missing}")

            for col_name, expected_type in expected_columns.items():
                if col_name not in db_columns:
                    continue
                db_type = db_columns[col_name]
                if not self._types_compatible(col_name, expected_type, db_type):
                    expected_type_names = ", ".join(
                        self._get_allowed_type_names(col_name, expected_type)
                    )
                    errors.append(
                        f"Table '{table_name}' column '{col_name}' has type "
                        f"{type(db_type).__name__}, expected {expected_type_names}"
                    )

        return errors

    def _types_compatible(self, column_name: str, expected: Any, actual: Any) -> bool:
        if isinstance(expected, TypeDecorator):
            expected = expected.impl

        if isinstance(expected, Enum) or isinstance(actual, Enum):
            if isinstance(expected, Enum) and isinstance(actual, _STRING_TYPES):
                return bool(actual.length == max(len(value) for value in expected.enums))

            return (
                isinstance(expected, Enum)
                and isinstance(actual, Enum)
                and set(expected.enums) == set(actual.enums)
            )

        allowed_types = self._ALLOWED_TYPES_BY_COLUMN.get(column_name)
        if allowed_types is not None and isinstance(actual, allowed_types):
            return True

        return type(expected) is type(actual)

    def _get_allowed_type_names(self, column_name: str, expected: Any) -> tuple[str, ...]:
        return self._ALLOWED_TYPE_NAMES_BY_COLUMN.get(
            column_name, (type(expected).__name__,)
        )
