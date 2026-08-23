from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Generator,
    Iterable,
    Iterator,
    MutableSequence,
    Sequence,
)
from types import NoneType, UnionType
from typing import Annotated, Any, Union, get_args, get_origin

_BATCH_CONTAINER_ORIGINS: tuple[Any, ...] = (
    list,
    tuple,
    Sequence,
    MutableSequence,
    Iterable,
    Iterator,
    AsyncIterable,
    AsyncIterator,
    Generator,
    AsyncGenerator,
)

_NONE_TITLE_PARTS: frozenset[str] = frozenset(
    {
        "none",
        "nonetype",
        "type[none]",
        "none_type",
    },
)


class NullExclusion:
    """NoneType exclusion for publisher AsyncAPI payload schemas."""

    def __init__(self, *, batch: bool, is_generator: bool) -> None:
        """Configure the excluded for a publisher context.

        Args:
            batch: Whether the publisher sends messages in batches, so the
                direct batch item type is cleaned too.
            is_generator: Whether the annotation is already unwrapped to the
                generator yield type (fast-depends does it for generators).
        """
        self._batch = batch
        self._is_generator = is_generator

    def exclude_from_annotation(self, annotation: Any) -> Any:
        """Exclude NoneType from a message-level annotation.

        Only the message-level positions are processed: the top-level `| None`
        union and, for batch publishers, the direct item type of a sequence
        container. Nested None types (e.g. `dict[str, int | None]` values) are
        preserved, mirroring the `skip_none` runtime behavior.

        Args:
            annotation: Python annotation describing the published message.

        Returns:
            The rebuilt annotation without NoneType parts, or `None` when the
            annotation describes nothing but `None` (nothing can be sent).
        """
        if annotation is None or annotation is NoneType:
            return None

        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin in {UnionType, Union}:
            return self._exclude_union(args)

        if origin is Annotated:
            return self._exclude_annotated(args)

        if self._is_batch_container(origin, args):
            return self._exclude_batch_item(origin, args[0])

        return annotation

    def exclude_from_schema(self, schema: dict[str, Any]) -> dict[str, Any] | None:
        """Remove null branches from a compiled payload schema.

        Only the message-level positions are processed: the schema root and,
        for batch publishers, the direct array item schema. Nested null types
        (e.g. `dict[str, int | None]` values) are preserved.

        This is a safety net for annotations which were not rebuilt by
        `exclude_from_annotation` (strings, exotic generics, pydantic
        version differences).

        Args:
            schema: Compiled JSON schema of the publisher payload.

        Returns:
            The cleaned schema, or `None` when nothing can be sent.
        """
        if (cleaned := self._strip_node(schema)) is None:
            return None

        if self._batch and not self._is_generator:
            return self._strip_items(cleaned)

        return cleaned

    def _exclude_union(self, args: tuple[Any, ...]) -> Any:
        """Drop NoneType members from a union, then rebuild it."""
        if not (kept := tuple(arg for arg in args if arg is not NoneType)):
            return None

        if len(kept) == 1:
            return self.exclude_from_annotation(kept[0])

        return Union.__getitem__(tuple(kept))

    def _exclude_annotated(self, args: tuple[Any, ...]) -> Any:
        """Clean the inner type, keep the metadata."""
        if (inner := self.exclude_from_annotation(args[0])) is None:
            return None

        # Keeps 3.10-compatible syntax: starred subscripts are 3.11+.
        return Annotated[(inner, *args[1:])]

    def _is_batch_container(self, origin: Any, args: tuple[Any, ...]) -> bool:
        """Check whether the annotation is a batch sequence to clean."""
        return (
            self._batch
            and not self._is_generator
            and origin is not None
            and origin in _BATCH_CONTAINER_ORIGINS
            and len(args) == 1
        )

    def _exclude_batch_item(self, origin: Any, item: Any) -> Any:
        """Each batch item is a single message: exclude None from it too."""
        # TODO: looks off: creating a sub-instance inside the class
        item = NullExclusion(batch=False, is_generator=False).exclude_from_annotation(
            item,
        )

        return origin[item] if item is not None else item

    def _strip_node(self, schema: dict[str, Any]) -> dict[str, Any] | None:
        """Clean one schema node: branches first, then the type list."""
        if self._is_null_schema(schema):
            return None

        cleaned = self._strip_branches(schema.copy())
        if cleaned is None:
            return None

        return self._strip_null_type(cleaned)

    def _strip_branches(self, schema: dict[str, Any]) -> dict[str, Any] | None:
        """Remove null branches from anyOf/oneOf unions."""
        for key in ("anyOf", "oneOf"):
            branches = schema.get(key)
            if not isinstance(branches, list):
                continue

            kept = [branch for branch in branches if not self._is_null_schema(branch)]
            if not kept:
                return None

            if len(kept) == len(branches):
                continue

            if len(kept) == 1:
                schema = {**schema, **kept[0]}
                schema.pop(key, None)

            else:
                schema[key] = kept

            schema = self._clean_title(schema, kept)

        return schema

    def _strip_null_type(self, schema: dict[str, Any]) -> dict[str, Any] | None:
        """Drop "null" from a `type: [...]` list."""
        schema_type = schema.get("type")
        if not isinstance(schema_type, list):
            return schema

        kept_types = [item for item in schema_type if item != "null"]
        if not kept_types:
            return None
        if len(kept_types) != len(schema_type):
            schema["type"] = kept_types[0] if len(kept_types) == 1 else kept_types

        return schema

    def _strip_items(self, schema: dict[str, Any]) -> dict[str, Any] | None:
        """Clean the direct items schema of a batch array."""
        items = schema.get("items")
        if not isinstance(items, dict):
            return schema

        items = self._strip_node(items)
        if items is None:
            return None
        schema["items"] = items
        return schema

    def _is_null_schema(self, schema: Any) -> bool:
        """Check whether a schema node describes the JSON null value only."""
        if not isinstance(schema, dict):
            return False

        schema_type = schema.get("type")
        return schema_type in ("null", ["null"])

    def _clean_title(
        self, schema: dict[str, Any], kept: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Normalize a schema title after NoneType branches were removed."""
        if "title" not in schema:
            return schema

        # A merged branch brought its own title, no cleanup needed.
        if len(kept) == 1 and kept[0].get("title"):
            return schema

        title = schema.get("title")
        if not isinstance(title, str):
            return schema

        cleaned = self._strip_none_from_title(title)
        if cleaned is None:
            schema.pop("title", None)

        else:
            schema["title"] = cleaned

        return schema

    def _strip_none_from_title(self, title: str) -> str | None:
        """Remove NoneType mentions from a schema title string."""
        parts = [part.strip() for part in title.split("|") if part.strip()]
        kept = [part for part in parts if part.lower() not in _NONE_TITLE_PARTS]

        if not kept:
            return None

        cleaned = " | ".join(kept)

        if cleaned.lower().startswith("optional[") and cleaned.endswith("]"):
            cleaned = cleaned[len("optional[") : -1]

        if cleaned.lower().startswith("union[") and cleaned.endswith("]"):
            inner = cleaned[len("union[") : -1]
            inner_parts = [part.strip() for part in inner.split(",") if part.strip()]
            inner_kept = [
                part for part in inner_parts if part.lower() not in _NONE_TITLE_PARTS
            ]
            if not inner_kept:
                return None
            cleaned = "Union[" + ", ".join(inner_kept) + "]"

        return cleaned
