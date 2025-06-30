from tests.asyncapi.confluent.security import SecurityTestcase

from .base import AsyncAPI26Mixin


class TestSecurity(AsyncAPI26Mixin, SecurityTestcase):
    pass
