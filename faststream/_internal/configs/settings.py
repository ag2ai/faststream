from typing import Any, TypeVar, overload

T = TypeVar('T')

class Settings:
    def __init__(self, key: str) -> None:
        self.key = key

class SettingsContainer:
    def __init__(self, **kwargs: Any) -> None:
        self._items: dict[str, Any] = dict(kwargs)

    @overload
    def resolve(self, item: Settings) -> Any:
        ...
    @overload
    def resolve(self, item: T) -> T:
        ...

    def resolve(self, item):
        if isinstance(item, Settings):
            return self._items.get(item.key)
        self.resolve_child(item)
        return item

    def resolve_child(self, item: Any) -> None:
        for attr_name in dir(item):
            attr = getattr(item, attr_name)
            if isinstance(attr, Settings):
                setattr(item, attr_name, self._items.get(attr.key))

    def resolve_recursion(self) -> None:
        pass