from tests.asyncapi.confluent.security import SecurityTestcase

from .base import AsyncAPI30Mixin


class TestSecurity(AsyncAPI30Mixin, SecurityTestcase):
    pass
