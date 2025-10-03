from dataclasses import dataclass
from typing import Any

from faststream._internal.configs.settings import RealSettingsContainer, Settings


def test_smoke() -> None:
    settings = RealSettingsContainer({"a": 1})
    assert settings.resolve(Settings("a")) == 1


def test_nested() -> None:
    @dataclass
    class SomeClass:
        field: Any

    obj = SomeClass(field=Settings("key"))

    settings = RealSettingsContainer({"key": 1})
    assert settings.resolve(obj) == SomeClass(field=1)


def test_deep_nested() -> None:
    @dataclass
    class SomeClass:
        field: Any

    obj = SomeClass(field=SomeClass(field=Settings("key")))

    settings = RealSettingsContainer({"key": 1})
    assert settings.resolve(obj) == SomeClass(field=SomeClass(field=1))


def test_circular_dependency() -> None:
    @dataclass
    class SomeClass:
        field: Any
        other: Any = None

    obj1 = SomeClass(field=Settings("key"))
    obj2 = SomeClass(field=Settings("key"))
    obj2.other = obj1
    obj1.other = obj2

    settings = RealSettingsContainer({"key": 1})
    assert settings.resolve(obj2) == SomeClass(field=1, other=obj1)
    assert settings.resolve(obj1) == SomeClass(field=1, other=obj2)
