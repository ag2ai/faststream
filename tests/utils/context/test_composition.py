import pytest

from faststream._internal.context.composition import ContextRepoComposition
from faststream._internal.context.repository import ContextRepo
from faststream.exceptions import ContextError


def test_context_property() -> None:
    lgh_context = ContextRepo({"key": "value"})
    rgh_context = ContextRepo({"key": "value"})
    context = ContextRepoComposition(lgh_context, rgh_context)

    assert context.context == {"key": "value", "context": context}


def test_context_property_with_priority() -> None:
    lgh_context = ContextRepo({"key": "lgh value"})
    rgh_context = ContextRepo({"key": "rgh value"})
    context = ContextRepoComposition(lgh_context, rgh_context)

    assert context.context == {"key": "rgh value", "context": context}


def test_set_global() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    context.set_global("key", "value")

    assert context.get("key") == "value"
    assert lgh_context.get("key") == "value"
    assert rgh_context.get("key") == "value"
    assert lgh_context._global_context == {"key": "value", "context": context}
    assert rgh_context._global_context == {"key": "value", "context": context}


def test_reset_global() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    context.set_global("key", "value")
    assert context.get("key") == "value"
    context.reset_global("key")

    assert context.get("key") is None
    assert lgh_context._global_context == {"context": context}
    assert rgh_context._global_context == {"context": context}


def test_set_local() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    context.set_local("key", "value")
    assert context.get_local("key") == "value"

    assert lgh_context._scope_context["key"].get() == "value"
    assert rgh_context._scope_context["key"].get() == "value"


def test_reset_local() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    variable = context.set_local("key", "value")

    assert context.get_local("key") == "value"
    assert lgh_context._scope_context["key"].get() == "value"
    assert rgh_context._scope_context["key"].get() == "value"

    context.reset_local("key", variable)
    assert "key" in lgh_context._scope_context
    assert "key" in rgh_context._scope_context


def test_get_local() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    context.set_local("key", "value")

    assert context.get_local("key") == "value"
    assert lgh_context._scope_context["key"].get() == "value"
    assert rgh_context._scope_context["key"].get() == "value"


def test_get_local_for_lgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    lgh_context.set_local("key", "value")

    assert context.get_local("key") == "value"
    assert lgh_context._scope_context["key"].get() == "value"
    assert "key" not in rgh_context._scope_context


def test_get_local_for_rgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    rgh_context.set_local("key", "value")

    assert context.get_local("key") == "value"
    assert "key" not in lgh_context._scope_context
    assert rgh_context._scope_context["key"].get() == "value"


def test_scope() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    with context.scope("key", "value"):
        assert context.get_local("key") == "value"


def test_scope_for_lgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    with lgh_context.scope("key", "value"):
        assert context.get_local("key") == "value"


def test_scope_for_rgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    with rgh_context.scope("key", "value"):
        assert context.get_local("key") == "value"


def test_get() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    context.set_global("key", "value")

    assert context.get("key", "value")
    assert lgh_context.get("key") == "value"
    assert rgh_context.get("key") == "value"


def test_get_for_lgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    lgh_context.set_global("key", "value")

    assert context.get("key", "value")
    assert lgh_context.get("key") == "value"
    assert rgh_context.get("key") is None


def test_get_for_rgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    rgh_context.set_global("key", "value")

    assert context.get("key", "value")
    assert lgh_context.get("key") is None
    assert rgh_context.get("key") == "value"


def test_getattr() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    context.set_global("key", "value")

    assert context.key == "value"


def test_getattr_for_lgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    lgh_context.set_global("key", "value")

    assert context.key == "value"


def test_getattr_for_rgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    rgh_context.set_global("key", "value")

    assert context.key == "value"


def test_resolve() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    context.set_global("key", {"deep_key": "value"})

    assert context.resolve("key.deep_key") == "value"


def test_resolve_for_lgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    lgh_context.set_global("key", {"deep_key": "value"})

    assert context.resolve("key.deep_key") == "value"


def test_resolve_for_rgh_context() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    rgh_context.set_global("key", {"deep_key": "value"})

    assert context.resolve("key.deep_key") == "value"


def test_resolve_failed() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    with pytest.raises(ContextError):
        context.resolve("key.deep_key")


def test_clear() -> None:
    lgh_context = ContextRepo()
    rgh_context = ContextRepo()
    context = ContextRepoComposition(lgh_context, rgh_context)

    context.set_global("key", "value")
    context.clear()

    assert context.context == {"context": context}
