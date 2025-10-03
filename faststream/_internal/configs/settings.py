from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class Settings:
    key: str


class SettingsContainer(Protocol):
    def resolve(self, item: Any) -> Any:
        pass


class RealSettingsContainer(SettingsContainer):
    def __init__(self, settings: Mapping[str, Any]) -> None:
        self._items = settings

    def resolve(self, item: Any) -> Any:
        if isinstance(item, Settings):
            return self._items[item.key]
        self._resolve_child(item)
        return item

    def _resolve_child(self, item: Any, seen: set[Any] | None = None) -> None:
        if seen is None:
            seen = set()

        if id(item) in seen:
            return

        seen.add(id(item))

        for attr_name in dir(item):
            if not attr_name.startswith("__"):
                attr = getattr(item, attr_name)
                if isinstance(attr, Settings):
                    setattr(item, attr_name, self._items[attr.key])
                self._resolve_child(attr, seen)


class FakeSettingsContainer(SettingsContainer):
    def resolve(self, item: Any) -> Any:
        return item


def make_settings_container(
    settings: Mapping[str, Any] | None = None,
) -> SettingsContainer:
    if not settings:
        return FakeSettingsContainer()
    return RealSettingsContainer(settings)
