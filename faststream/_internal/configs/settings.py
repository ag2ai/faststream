from typing import Any


class Settings:
    def __init__(self, key: str) -> None:
        self.key = key


class SettingsContainer:
    def __init__(self, **kwargs: Any) -> None:
        self._items = dict(kwargs)

    def resolve_from(self, item: Any) -> Any:
        if isinstance(item, Settings):
            return self._items.get(item.key)
        return item
