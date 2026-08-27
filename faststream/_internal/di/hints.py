from typing import TYPE_CHECKING, Any

from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fast_depends.core import CallModel


# Driver class -> (module holding the annotation, its name there).
# Populated by every broker's `annotations` module at import time.
_CONTEXT_ANNOTATIONS: dict[type[Any], tuple[str, str]] = {}


def register_context_annotations(
    module: str,
    annotations: "Mapping[type[Any], str]",
) -> None:
    """Register the driver classes a broker wraps in `Context` annotations.

    Args:
        module: `__name__` of the broker's `annotations` module.
        annotations: driver class to the name of the annotation wrapping it.
    """
    for driver_type, name in annotations.items():
        _CONTEXT_ANNOTATIONS[driver_type] = (module, name)


def check_context_annotations(model: "CallModel") -> None:
    """Reject handler arguments annotated with a broker's own driver class.

    `model.params` holds the arguments fast-depends treats as message fields,
    so a `Context`- or `Depends`-annotated argument never reaches here.
    """
    hints = [
        _format_hint(model.call_name, option.field_name, option.field_type)
        for option in model.params
        if isinstance(option.field_type, type)
        and option.field_type in _CONTEXT_ANNOTATIONS
    ]

    if hints:
        raise SetupError("\n".join(hints))


def _format_hint(call_name: str, field_name: str, driver_type: type[Any]) -> str:
    module, name = _CONTEXT_ANNOTATIONS[driver_type]
    driver_path = f"{driver_type.__module__}.{driver_type.__qualname__}"

    return (
        f"`{call_name}` parameter `{field_name}` is annotated with "
        f"`{driver_path}`, which FastStream cannot inject.\n"
        "Use the context annotation instead:\n"
        f"\n    from {module} import {name}\n"
    )
