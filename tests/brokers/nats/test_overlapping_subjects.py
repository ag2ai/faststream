import pytest

from faststream.nats.schemas.js_stream import JStream

TEST_CASES = {
    "single_subject": (
        JStream("test", subjects=["a"]),
        {"a"},
    ),
    "duplicate_subject": (
        JStream("test", subjects=["a", "a"]),
        {"a"},
    ),
    "duplicate_nested_subject": (
        JStream("test", subjects=["a.b", "a.b"]),
        {"a.b"},
    ),
    "different_subjects": (
        JStream("test", subjects=["a", "b"]),
        {"a", "b"},
    ),
    "different_nested_subjects": (
        JStream("test", subjects=["a.b", "b.b"]),
        {"a.b", "b.b"},
    ),
    "nested_and_wildcard": (
        JStream("test", subjects=["a.b", "a.*"]),
        {"a.*"},
    ),
    "deep_nested_and_wildcard": (
        JStream("test", subjects=["a.b.c", "a.*.c"]),
        {"a.*.c"},
    ),
    "overlapping_wildcards_and_specific": (
        JStream("test", subjects=["*.b.c", "a.>", "a.b.c"]),
        {"a.>", "*.b.c"},
    ),
    "nested_wildcard_and_specific": (
        JStream("test", subjects=["a.b", "a.*", "a.b.c"]),
        {"a.b.c", "a.*"},
    ),
    "wildcard_overlaps_specific": (
        JStream("test", subjects=["a.b", "a.>", "a.b.c"]),
        {"a.>"},
    ),
    "wildcard_overlaps_wildcard": (
        JStream("test", subjects=["a.*", "a.>"]),
        {"a.>"},
    ),
    "wildcard_overlaps_wildcard_reversed": (
        JStream("test", subjects=["a.>", "a.*"]),
        {"a.>"},
    ),
    "wildcard_overlaps_wildcard_and_specific": (
        JStream("test", subjects=["a.*", "a.>", "a.b"]),
        {"a.>"},
    ),
    "specific_wildcard_overlaps_wildcard": (
        JStream("test", subjects=["a.b", "a.*", "a.>"]),
        {"a.>"},
    ),
    "deep_wildcard_overlaps_wildcard": (
        JStream("test", subjects=["a.*.*", "a.>"]),
        {"a.>"},
    ),
    "wildcard_overlaps_deep_wildcards": (
        JStream(
            "test",
            subjects=[
                "a.*.*",
                "a.*.*.*",
                "a.b.c",
                "a.>",
                "a.b.c.d",
            ],
        ),
        {"a.>"},
    ),
    "deep_wildcards": (
        JStream("test", subjects=["a.*.*", "a.*.*.*", "a.b.c", "a.b.c.d"]),
        {"a.*.*.*", "a.*.*"},
    ),
}


@pytest.mark.parametrize(
    ("stream", "subjects"),
    TEST_CASES.values(),
    ids=TEST_CASES.keys(),
)
def test_filter_overlapped_subjects(stream: JStream, subjects: set[str]) -> None:
    assert set(stream.subjects) == subjects
