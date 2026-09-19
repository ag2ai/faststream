import subprocess
import sys


def test_import_faststream_leaves_test_brokers_out() -> None:
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
