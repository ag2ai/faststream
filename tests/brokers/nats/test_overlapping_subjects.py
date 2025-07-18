import pytest

from faststream.nats.schemas.js_stream import SubjectsCollection

TEST_CASES = {
    "single_subject": (
        SubjectsCollection(["a"]),
        {"a"},
    ),
    "duplicate_subject": (
        SubjectsCollection(["a", "a"]),
        {"a"},
    ),
    "duplicate_nested_subject": (
        SubjectsCollection(["a.b", "a.b"]),
        {"a.b"},
    ),
    "different_subjects": (
        SubjectsCollection(["a", "b"]),
        {"a", "b"},
    ),
    "different_nested_subjects": (
        SubjectsCollection(["a.b", "b.b"]),
        {"a.b", "b.b"},
    ),
    "nested_and_wildcard": (
        SubjectsCollection(["a.b", "a.*"]),
        {"a.*"},
    ),
    "deep_nested_and_wildcard": (
        SubjectsCollection(["a.b.c", "a.*.c"]),
        {"a.*.c"},
    ),
    "overlapping_wildcards_and_specific": (
        SubjectsCollection(["*.b.c", "a.>", "a.b.c"]),
        {"a.>", "*.b.c"},
    ),
    "nested_wildcard_and_specific": (
        SubjectsCollection(["a.b", "a.*", "a.b.c"]),
        {"a.b.c", "a.*"},
    ),
    "wildcard_overlaps_specific": (
        SubjectsCollection(["a.b", "a.>", "a.b.c"]),
        {"a.>"},
    ),
    "wildcard_overlaps_wildcard": (
        SubjectsCollection(["a.*", "a.>"]),
        {"a.>"},
    ),
    "wildcard_overlaps_wildcard_reversed": (
        SubjectsCollection(["a.>", "a.*"]),
        {"a.>"},
    ),
    "wildcard_overlaps_wildcard_and_specific": (
        SubjectsCollection(["a.*", "a.>", "a.b"]),
        {"a.>"},
    ),
    "specific_wildcard_overlaps_wildcard": (
        SubjectsCollection(["a.b", "a.*", "a.>"]),
        {"a.>"},
    ),
    "deep_wildcard_overlaps_wildcard": (
        SubjectsCollection(["a.*.*", "a.>"]),
        {"a.>"},
    ),
    "wildcard_overlaps_deep_wildcards": (
        SubjectsCollection(["a.*.*", "a.*.*.*", "a.b.c", "a.>", "a.b.c.d"]),
        {"a.>"},
    ),
    "deep_wildcards": (
        SubjectsCollection(["a.*.*", "a.*.*.*", "a.b.c", "a.b.c.d"]),
        {"a.*.*.*", "a.*.*"},
    ),
}


@pytest.mark.parametrize(
    ("subjects", "expected"),
    TEST_CASES.values(),
    ids=TEST_CASES.keys(),
)
def test_filter_overlapped_subjects(
    subjects: SubjectsCollection, expected: set[str]
) -> None:
    assert set(subjects) == expected
