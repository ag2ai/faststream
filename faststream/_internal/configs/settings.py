from typing import Any


class Settings:
    def __init__(self, key: str) -> None:
        self.key = key


class SettingsContainer:
    def __init__(self, **kwargs: Any) -> None:
        self._items = dict(kwargs)

    # Возиожно просто возвращать из self,items
    def resolve_from(self, item: Any, outer: Any) -> Any:
        if isinstance(item, Settings):
            return outer.settings.get(item.key)
        return item

