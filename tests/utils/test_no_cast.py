from faststream import apply_types
from faststream.params import NoCast


def test_no_cast() -> None:
    @apply_types
    def handler(s: NoCast[str]) -> None:
        assert isinstance(s, int)

    handler(1)
