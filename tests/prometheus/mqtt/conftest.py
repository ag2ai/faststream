import pytest


@pytest.fixture(params=["3.1.1", "5.0"])
def mqtt_version(request: pytest.FixtureRequest) -> str:
    return request.param
