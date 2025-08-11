from tests.marks import pydantic_v1


@pydantic_v1
def test_exists_and_valid() -> None:
    from docs.docs_src.getting_started.config.settings_base_1 import settings

    assert settings.queue == "test-queue"
