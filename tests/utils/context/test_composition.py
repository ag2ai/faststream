import pytest

from faststream._internal.context.composition import ContextRepoComposition
from faststream._internal.context.repository import ContextRepo
from faststream.exceptions import ContextError


def test_context() -> None:
    context1 = ContextRepo()
    context2 = ContextRepo()
    context = ContextRepoComposition(context1, context2)

    context1.set_global("key", "value1")
    context2.set_global("key", "value2")

    assert context.context == {"key": "value1", "context": context}


def test_error_resolve() -> None:
    context = ContextRepoComposition()

    with pytest.raises(ContextError):
        context.resolve("key1.key2")


def test_getattr() -> None:
    context1 = ContextRepo()
    context2 = ContextRepo()
    context = ContextRepoComposition(context1, context2)

    context1.set_global("key1", "value1")
    context2.set_global("key2", "value2")

    assert context.key1 == "value1"
    assert context.key2 == "value2"


def test_getattr_undefined_attr() -> None:
    context = ContextRepoComposition()

    assert context.undefined_attr is None
