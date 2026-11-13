import subprocess
import sys

import pytest

BROKERS = (
    pytest.param("kafka", "aiokafka", marks=pytest.mark.kafka()),
    pytest.param("confluent", "confluent_kafka", marks=pytest.mark.confluent()),
    pytest.param("rabbit", "aio_pika", marks=pytest.mark.rabbit()),
    pytest.param("nats", "nats", marks=pytest.mark.nats()),
    pytest.param("redis", "redis", marks=pytest.mark.redis()),
    pytest.param("mqtt", "zmqtt", marks=pytest.mark.mqtt()),
)


def import_broker(broker: str, setup: str) -> str:
    # a fresh interpreter: `sys.modules` is patched before the first import
    result = subprocess.run(
        [sys.executable, "-c", f"{setup}\nimport faststream.{broker}"],
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return result.stderr


@pytest.mark.parametrize(("broker", "package"), BROKERS)
def test_missing_package_points_at_the_extra(broker: str, package: str) -> None:
    stderr = import_broker(broker, f"import sys; sys.modules['{package}'] = None")

    assert f'pip install "faststream[{broker}]"' in stderr


@pytest.mark.parametrize(("broker", "package"), BROKERS)
def test_broken_package_keeps_its_own_error(broker: str, package: str) -> None:
    # an installed package that fails to import a name it should have
    setup = (
        "import sys, types; from importlib.machinery import ModuleSpec\n"
        f"m = types.ModuleType('{package}'); m.__path__ = []\n"
        f"m.__spec__ = ModuleSpec('{package}', None, is_package=True)\n"
        f"sys.modules['{package}'] = m"
    )

    stderr = import_broker(broker, setup)

    assert "Error" in stderr
    assert "pip install" not in stderr
