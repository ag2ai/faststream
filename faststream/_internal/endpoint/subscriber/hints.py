from collections.abc import Iterator
from types import UnionType
from typing import TYPE_CHECKING, Annotated, Any, Union, get_args, get_origin

from faststream._internal._compat import ExceptionGroup
from faststream._internal.configs import UnderlyingDriverAnnotation
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fast_depends.core import CallModel
    from fast_depends.library.serializer import OptionItem


def check_context_annotations(
    dependent: "CallModel",
    annotations: "Mapping[Any, UnderlyingDriverAnnotation | Any]",
) -> None:
    """Reject call model arguments annotated with a broker's own driver class.

    Context annotations never reach the model's params, so any driver class
    found there would be validated as message data.

    Args:
        dependent: the handler's call model, dependencies included.
        annotations: driver type hint to the context annotation replacing it. An
            `UnderlyingDriverAnnotation` value also names the import to suggest.
    """
    messages = [
        _format_hint(option.field_name, hint, annotations[hint], dependency)
        for dependency, option in _options(dependent)
        if (hint := _find_mapped(option.field_type, annotations)) is not None
    ]
    # a dependency shared by several arguments is still one mistake
    errors = [SetupError(m) for m in dict.fromkeys(messages)]

    if len(errors) == 1:
        raise errors[0]

    if errors:
        msg = f"`{dependent.call_name}` has arguments FastStream cannot inject."
        raise ExceptionGroup(msg, errors)


def _options(
    model: "CallModel",
    dependency: str | None = None,
) -> Iterator[tuple[str | None, "OptionItem"]]:
    for option in model.params:
        yield dependency, option

    for key in (*model.dependencies.values(), *model.extra_dependencies):
        sub_model = model.dependency_provider.get_dependant(key)
        yield from _options(sub_model, sub_model.call_name)


def _find_mapped(hint: Any, annotations: "Mapping[Any, Any]") -> Any:
    origin = get_origin(hint)

    if origin is Annotated:
        return _find_mapped(get_args(hint)[0], annotations)

    if origin is Union or origin is UnionType:
        for arg in get_args(hint):
            if (mapped := _find_mapped(arg, annotations)) is not None:
                return mapped
        return None

    try:
        return hint if hint in annotations else None
    except TypeError:
        # An unhashable hint cannot be a key, so it cannot be mapped.
        return None


def _format_hint(
    field_name: str,
    hint: Any,
    annotation: Any,
    dependency: str | None,
) -> str:
    owner = f" of dependency `{dependency}`" if dependency else ""
    message = (
        f"`{field_name}`{owner} is annotated with `{_describe(hint)}`, "
        "which FastStream cannot inject.\n"
    )

    if isinstance(annotation, UnderlyingDriverAnnotation):
        return (
            f"{message}Use the context annotation instead:\n"
            f"\n    from {annotation.module} import {annotation.name}\n"
        )

    return f"{message}Use the context annotation FastStream provides for it instead."


def _describe(hint: Any) -> str:
    if isinstance(hint, type):
        return f"{hint.__module__}.{hint.__qualname__}"

    return str(hint)
