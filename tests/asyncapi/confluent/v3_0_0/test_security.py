import pytest

from tests.asyncapi.confluent.security import SecurityTestcase

from .base import AsyncAPI30Mixin


@pytest.mark.confluent()
class TestSecurity(AsyncAPI30Mixin, SecurityTestcase):
    pass
