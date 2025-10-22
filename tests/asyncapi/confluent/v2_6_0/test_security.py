import pytest
from dirty_equals import IsPartialDict

from faststream.confluent import KafkaBroker
from faststream.security import (
    SASLGSSAPI,
    BaseSecurity,
    SASLOAuthBearer,
    SASLPlaintext,
    SASLScram256,
    SASLScram512,
)

from .base import AsyncAPI26Mixin


class SecurityTestcase(AsyncAPI26Mixin):
    @pytest.mark.parametrize(
        ("security", "schema"),
        (
            pytest.param(
                BaseSecurity(use_ssl=True),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "protocolVersion": "auto",
                            "security": [],
                        }),
                    }),
                }),
                id="BaseSecurity",
            ),
            pytest.param(
                SASLPlaintext(
                    username="admin",
                    password="password",
                    use_ssl=True,
                ),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "security": [
                                {"$ref": "#/components/securitySchemes/user-password"}
                            ],
                        }),
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {
                            "user-password": {"type": "userPassword"},
                        },
                    }),
                }),
                id="SASLPlaintext",
            ),
            pytest.param(
                SASLScram256(
                    username="admin",
                    password="password",
                    use_ssl=True,
                ),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "security": [
                                {"$ref": "#/components/securitySchemes/scram256"}
                            ],
                        }),
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {
                            "scram256": {"type": "scramSha256"},
                        },
                    }),
                }),
                id="SASLScram256",
            ),
            pytest.param(
                SASLScram512(
                    username="admin",
                    password="password",
                    use_ssl=True,
                ),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "security": [
                                {"$ref": "#/components/securitySchemes/scram512"}
                            ],
                        }),
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {
                            "scram512": {"type": "scramSha512"},
                        },
                    }),
                }),
                id="SASLScram512",
            ),
            pytest.param(
                SASLOAuthBearer(use_ssl=True),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "security": [
                                {"$ref": "#/components/securitySchemes/oauthbearer"}
                            ],
                        }),
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {
                            "oauthbearer": {"type": "oauth2", "$ref": ""},
                        },
                    }),
                }),
                id="SASLOAuthBearer",
            ),
            pytest.param(
                SASLGSSAPI(use_ssl=True),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "security": [{"$ref": "#/components/securitySchemes/gssapi"}],
                        }),
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {"gssapi": {"type": "gssapi"}},
                    }),
                }),
                id="SASLGSSAPI",
            ),
        ),
    )
    def test_security_schema(
        self,
        security: BaseSecurity,
        schema: dict[str, str],
    ) -> None:
        broker = KafkaBroker(security=security)
        generated_schema = self.get_schema(broker)
        assert generated_schema.to_jsonable() == schema
