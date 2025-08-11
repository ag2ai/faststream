import pytest

from tests.asyncapi.confluent.security import SecurityTestcase

from .base import AsyncAPI26Mixin


@pytest.mark.confluent()
class TestSecurity(AsyncAPI26Mixin, SecurityTestcase):
    pass
