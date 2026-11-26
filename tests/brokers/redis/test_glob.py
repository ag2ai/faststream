import pytest

from faststream.redis.testing import _redis_glob_matches

pytestmark = pytest.mark.redis()


@pytest.mark.parametrize(
    ("pattern", "channel", "expected"),
    (
        # `*` and empty subjects: Redis never enters its matching loop when the
        # subject is empty, so even a bare `*` does not match "".
        ("", "", True),
        ("*", "", False),
        ("*", "abc", True),
        ("*", "a.b", True),
        ("a*", "a", True),
        ("a*b", "ab", True),
        ("a*b", "axxb", True),
        ("a**b", "ab", True),
        pytest.param("*a" * 1000, "a" * 1000, True, id="star-depth-1000"),
        # `?` matches exactly one byte.
        ("?", "a", True),
        ("?", "", False),
        ("?", "ab", False),
        ("?", "é", False),
        ("??", "é", True),
        ("?*", "", False),
        # Character classes.
        ("[ab]", "a", True),
        ("[ab]", "b", True),
        ("[ab]", "c", False),
        ("[ab]", "ab", False),
        ("[^ab]", "c", True),
        ("[^ab]", "a", False),
        ("[^ab]", "ab", False),
        ("[a-z]", "m", True),
        ("[a-z]", "M", False),
        ("[]", "a", False),
        ("[]", "]", False),
        ("[^]", "a", True),
        ("[^]", "]", True),
        ("[^]", "é", False),
        ("[a-]", "]", True),
        ("[a-]", "^", True),
        ("[a-]", "_", True),
        ("[a-]", "`", True),
        ("[a-]", "a", True),
        ("[a-]", "-", False),
        ("[a-]", "b", False),
        ("[-a]", "-", True),
        ("[-a]", "a", True),
        ("[-a]", "b", False),
        ("[z-a]", "m", True),
        ("[z-a]", "z", True),
        ("[z-a]", "A", False),
        # `!` is literal and `^` negates only right after `[`.
        ("[!a]", "!", True),
        ("[!a]", "a", True),
        ("[!a]", "b", False),
        ("[a^]", "^", True),
        ("[a^]", "a", True),
        ("[a^]", "b", False),
        # Signed-char builds read the non-ASCII bound as a negative value.
        ("[a-é]", "b", False),
        ("[\\]]", "]", True),
        ("[\\]]", "a", False),
        # An unterminated class ends with the pattern.
        ("[a", "a", True),
        ("[a", "b", False),
        # Escapes.
        ("\\*", "*", True),
        ("\\*", "a", False),
        ("\\?", "?", True),
        ("abc\\", "abc\\", True),
        ("abc\\", "abc", False),
        # Regex metacharacters stay literal.
        (".", ".", True),
        (".", "a", False),
        ("+", "+", True),
        ("+", "a", False),
        ("(", "(", True),
        ("(", "a", False),
        (")", ")", True),
        (")", "a", False),
        ("|", "|", True),
        ("|", "a", False),
        ("$", "$", True),
        ("$", "a", False),
        ("^", "^", True),
        ("^", "a", False),
        ("{2}", "{2}", True),
        ("{2}", "22", False),
        ("}", "}", True),
        # Matching is anchored to the whole channel.
        ("a", "ab", False),
        ("a", "xa", False),
        ("ab", "ab", True),
    ),
)
def test_redis_glob_matches(pattern: str, channel: str, expected: bool) -> None:
    assert _redis_glob_matches(pattern, channel) is expected


def test_redis_glob_matches_uses_independent_encodings() -> None:
    # Redis encodes the subscriber's pattern and the publisher's channel with
    # their own connection encodings.
    assert _redis_glob_matches("logs.é?", "logs.éa", "latin-1", "utf-8") is False
    assert _redis_glob_matches("logs.é?", "logs.éa", "latin-1", "latin-1") is True


@pytest.mark.timeout(10)
def test_redis_glob_matches_pathological_pattern_quickly() -> None:
    assert _redis_glob_matches("*a*a*a*a*a*a*a*a*b", "a" * 5000) is False


def test_redis_glob_matches_aborts_on_deep_nesting() -> None:
    # Redis stops once `*` matching nests deeper than 1000.
    assert _redis_glob_matches("*a" * 1001, "a" * 2000) is False
