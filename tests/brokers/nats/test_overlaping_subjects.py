from typing import Final, List, Set

import pytest

from faststream.nats.broker.broker import filter_overlapped_subjects

TEST_CASES: Final = {
    "single_subject": (["a"], {"a"}),
    "duplicate_subject": (["a", "a"], {"a"}),
    "duplicate_nested_subject": (["a.b", "a.b"], {"a.b"}),
    "different_subjects": (["a", "b"], {"a", "b"}),
    "different_nested_subjects": (["a.b", "b.b"], {"a.b", "b.b"}),
    "nested_and_wildcard": (["a.b", "a.*"], {"a.*"}),
    "deep_nested_and_wildcard": (["a.b.c", "a.*.c"], {"a.*.c"}),
    "overlapping_wildcards_and_specific": (
        ["*.b.c", "a.>", "a.b.c"],
        {"a.>", "*.b.c"},
    ),
    "nested_wildcard_and_specific": (["a.b", "a.*", "a.b.c"], {"a.b.c", "a.*"}),
    "wildcard_overlaps_specific": (["a.b", "a.>", "a.b.c"], {"a.>"}),
    "wildcard_overlaps_wildcard": (["a.*", "a.>"], {"a.>"}),
    "wildcard_overlaps_wildcard_reversed": (["a.>", "a.*"], {"a.>"}),
    "wildcard_overlaps_wildcard_and_specific": (["a.*", "a.>", "a.b"], {"a.>"}),
    "specific_wildcard_overlaps_wildcard": (["a.b", "a.*", "a.>"], {"a.>"}),
    "deep_wildcard_overlaps_wildcard": (["a.*.*", "a.>"], {"a.>"}),
    "wildcard_overlaps_deep_wildcards": (
        [
            "a.*.*",
            "a.*.*.*",
            "a.b.c",
            "a.>",
            "a.b.c.d",
        ],
        {"a.>"},
    ),
    "deep_wildcards": (["a.*.*", "a.*.*.*", "a.b.c", "a.b.c.d"], {"a.*.*.*", "a.*.*"}),
}


@pytest.mark.parametrize(
    ("subjects", "expected"),
    TEST_CASES.values(),
    ids=TEST_CASES.keys(),
)
def test_filter_overlapped_subjects(subjects: List[str], expected: Set[str]) -> None:
    assert set(filter_overlapped_subjects(subjects)) == expected
