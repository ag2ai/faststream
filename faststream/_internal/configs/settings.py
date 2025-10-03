from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class Settings:
    key: str


class SettingsContainer:
    def __init__(self, **kwargs: Any) -> None:
        self._items: dict[str, Any] = dict(kwargs)

    def resolve(self, item: Any) -> Any:
        if isinstance(item, Settings):
            return self._items.get(item.key)
        self._resolve_child(item)
        return item

    def _resolve_child(self, item: Any) -> None:
        for attr_name in dir(item):
            if not attr_name.startswith("__"):
                attr = getattr(item, attr_name)
                if isinstance(attr, Settings):
                    setattr(item, attr_name, self._items.get(attr.key))
