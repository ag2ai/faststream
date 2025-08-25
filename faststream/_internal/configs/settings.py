from typing import Any


class Settings:
    def __init__(self, key: str | None = None, **kwargs: Any) -> None:
        self.key = key
        self._items = dict(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        return self._items.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._items[key]
    
    # example
    def resolve_from(self, outer: Any) -> Any:
        if self.key is not None:
            return outer.settings.get(self.key)
        return self._items

