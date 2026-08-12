from typing import Any

import pytest

from faststream.mqtt import MQTTBroker


@pytest.mark.parametrize(
    ("url", "expected_options", "expected_specification_url"),
    (
        (
            "mqtt://localhost",
            {
                "host": "localhost",
                "port": 1883,
                "username": None,
                "password": None,
                "tls": False,
            },
            "mqtt://localhost:1883",
        ),
        (
            "mqtts://localhost",
            {
                "host": "localhost",
                "port": 8883,
                "username": None,
                "password": None,
                "tls": True,
            },
            "mqtts://localhost:8883",
        ),
        (
            "mqtts://device:public@broker:8884",
            {
                "host": "broker",
                "port": 8884,
                "username": "device",
                "password": "p@ss",
                "tls": True,
            },
            "mqtts://broker:8884",
        ),
        (
            "mqtt://[::1]:1884",
            {
                "host": "::1",
                "port": 1884,
                "username": None,
                "password": None,
                "tls": False,
            },
            "mqtt://[::1]:1884",
        ),
        (
            "localhost:1884",
            {
                "host": "localhost",
                "port": 1884,
                "username": None,
                "password": None,
                "tls": False,
            },
            "mqtt://localhost:1884",
        ),
    ),
)
@pytest.mark.mqtt()
def test_url(
    url: str,
    expected_options: dict[str, Any],
    expected_specification_url: str,
) -> None:
    broker = MQTTBroker(url)

    for option, value in expected_options.items():
        assert broker._connection_kwargs[option] == value
    assert broker.specification.url == [expected_specification_url]
    assert broker.specification.protocol == expected_specification_url.split(":", 1)[0]


@pytest.mark.mqtt()
def test_legacy_host_and_port() -> None:
    broker = MQTTBroker(host="legacy", port=1884)

    assert broker._connection_kwargs["host"] == "legacy"
    assert broker._connection_kwargs["port"] == 1884
