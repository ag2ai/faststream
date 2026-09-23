import subprocess
import sys


def test_import_faststream_leaves_test_brokers_out() -> None:
    """A bare `import faststream` must not load the TestBroker machinery.

    `faststream._internal.testing.broker` imports `unittest.mock`, so loading it
    puts test-only modules into every production process. It got there once
    through `faststream.asgi`: the try-it-out factory imported `find_test_broker`
    at module level (#3163). That import is lazy now, and this test keeps it so.

    Nothing else notices the regression: tests, mypy and ruff all pass with the
    import hoisted back. The check runs in a fresh interpreter because the pytest
    session itself has imported every TestBroker long before this test starts.
    """
    code = (
        "import sys, faststream;"
        "print('faststream._internal.testing.broker' in sys.modules)"
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "False"
