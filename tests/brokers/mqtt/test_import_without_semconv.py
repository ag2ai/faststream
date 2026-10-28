import subprocess
import sys

import pytest


@pytest.mark.mqtt()
def test_mqtt_config_imports_without_semantic_conventions() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            """\
import sys
sys.modules["opentelemetry.semconv"] = None
import opentelemetry
from faststream.mqtt.broker.config import MQTTBrokerConfig
assert MQTTBrokerConfig is not None
""",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
