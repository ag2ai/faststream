from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import pytest

import faststream

OPTIONAL_DEPENDENCIES = {
    "cli": ("typer", "watchfiles"),
    "confluent": ("confluent-kafka",),
    "fastapi": ("fastapi",),
    "kafka": ("aiokafka",),
    "mqtt": ("zmqtt",),
    "nats": ("nats-py",),
    "opentelemetry": ("opentelemetry-sdk",),
    "prometheus": ("prometheus-client",),
    "rabbit": ("aio-pika",),
    "redis": ("redis",),
}


def _public_modules() -> list[str]:
    root = Path(faststream.__file__).parent
    names = []
    for source in sorted(root.rglob("*.py")):
        parts = source.relative_to(root).with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if all(not part.startswith("_") for part in parts):
            names.append(".".join((faststream.__name__, *parts)))
    return names


@pytest.mark.parametrize("module_name", _public_modules())
def test_all_resolves_at_runtime(module_name: str) -> None:
    """Every name in `__all__` must exist at runtime, not only for type checkers.

    A name imported under `if TYPE_CHECKING:` satisfies mypy and raises
    AttributeError in production - see #2841 -> #2898, where the API reference
    builder broke on `ConfluentParserType`.
    """
    try:
        for part in module_name.split("."):
            for dependency in OPTIONAL_DEPENDENCIES.get(part, ()):
                version(dependency)
    except PackageNotFoundError as exc:
        pytest.skip(f"{module_name} requires {exc.name}")

    module = import_module(module_name)

    for name in getattr(module, "__all__", ()):
        assert hasattr(module, name), (
            f"{module_name}.__all__ exports {name!r}, which does not exist at runtime"
        )
