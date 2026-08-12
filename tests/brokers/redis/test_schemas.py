import pytest

from faststream.redis import StreamSub


@pytest.mark.redis()
def test_stream_group() -> None:
    with pytest.raises(ValueError):  # noqa: PT011
        StreamSub("test", group="group")

    with pytest.raises(ValueError):  # noqa: PT011
        StreamSub("test", consumer="consumer")

    StreamSub("test", group="group", consumer="consumer")


@pytest.mark.redis()
def test_stream_declare_has_no_effect_without_group() -> None:
    with pytest.warns(RuntimeWarning, match="`declare` has no effect"):
        StreamSub("test", declare=False)
