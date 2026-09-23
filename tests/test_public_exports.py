import pkgutil
from importlib import import_module
from types import ModuleType

import pytest

import faststream


def _public_modules() -> list[str]:
    names = [faststream.__name__]
    for info in pkgutil.walk_packages(faststream.__path__, f"{faststream.__name__}."):
        # `_internal` and other private subtrees are not part of the public surface
        if any(part.startswith("_") for part in info.name.split(".")):
            continue
        names.append(info.name)
    return names


def _import(name: str) -> ModuleType:
    try:
        return import_module(name)
    except ImportError as e:
        # an optional dependency is missing in this environment
        pytest.skip(f"{name}: {e}")


@pytest.mark.parametrize("module_name", _public_modules())
def test_all_resolves_at_runtime(module_name: str) -> None:
    """Every name in `__all__` must exist at runtime, not only for type checkers.

    A name imported under `if TYPE_CHECKING:` satisfies mypy and raises
    AttributeError in production - see #2841 -> #2898, where the API reference
    builder broke on `ConfluentParserType`.
    """
    module = _import(module_name)

    for name in getattr(module, "__all__", ()):
        assert hasattr(module, name), (
            f"{module_name}.__all__ exports {name!r}, which does not exist at runtime"
        )
