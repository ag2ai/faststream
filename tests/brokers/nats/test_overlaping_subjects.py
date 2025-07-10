import pytest

from faststream.nats.broker.broker import filter_overlapped_subjects


@pytest.mark.parametrize(
    ("subjects", "expected"),
    [
        (["a"], {"a"}),
        (["a", "a"], {"a"}),
        (["a.b", "a.b"], {"a.b"}),
        (["a", "b"], {"a", "b"}),
        (["a.b", "b.b"], {"a.b", "b.b"}),
        (["a.b", "a.*"], {"a.*"}),
        (["a.b.c", "a.*.c"], {"a.*.c"}),
        (
            ["*.b.c", "a.>", "a.b.c"],
            {"*.b.c", "a.>"},
        ),  # <- this case will raise subject "a.>" overlaps with "*.b.c"
        (
            ["a.b", "a.*", "a.b.c"],
            {"a.*", "a.b.c"},
        ),  # but I don't know what to do with it
        (["a.b", "a.>", "a.b.c"], {"a.>"}),
        (["a.*", "a.>"], {"a.>", "a.*"}),
        (["a.*", "a.>", "a.b"], {"a.>", "a.*"}),
        (["a.*.*", "a.>"], {"a.>"}),
        (["a.*.*", "a.*.*.*", "a.b.c", "a.b.c.d", "a.>"], {"a.>"}),
        (["a.>", "a.*.*", "a.*.*.*", "a.b.c", "a.b.c.d"], {"a.>"}),
        (["a.*.*", "a.*.*.*", "a.b.c", "a.b.c.d"], {"a.*.*", "a.*.*.*"}),
    ],
)
def test_filter_overlapped_subjects(subjects: list[str], expected: set[str]) -> None:
    assert set(filter_overlapped_subjects(subjects)) == expected
