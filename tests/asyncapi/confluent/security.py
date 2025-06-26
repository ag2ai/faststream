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
from faststream.specification.base.specification import Specification


class SecurityTestcase:
    def get_schema(self, broker: KafkaBroker) -> Specification:
        raise NotImplementedError

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
                        })
                    })
                }),
                id="BaseSecurity",
            ),
            pytest.param(
                SASLPlaintext(
                    username="admin",
                    password="password",  # pragma: allowlist secret
                    use_ssl=True,
                ),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "security": [{"user-password": []}],
                        })
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {
                            "user-password": {"type": "userPassword"},
                        }
                    }),
                }),
                id="SASLPlaintext",
            ),
            pytest.param(
                SASLScram256(
                    username="admin",
                    password="password",  # pragma: allowlist secret
                    use_ssl=True,
                ),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "security": [{"scram256": []}],
                        })
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {
                            "scram256": {"type": "scramSha256"},
                        }
                    }),
                }),
                id="SASLScram256",
            ),
            pytest.param(
                SASLScram512(
                    username="admin",
                    password="password",  # pragma: allowlist secret
                    use_ssl=True,
                ),
                IsPartialDict({
                    "servers": IsPartialDict({
                        "development": IsPartialDict({
                            "protocol": "kafka-secure",
                            "security": [{"scram512": []}],
                        })
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {
                            "scram512": {"type": "scramSha512"},
                        }
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
                            "security": [{"oauthbearer": []}],
                        })
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {
                            "oauthbearer": {"type": "oauth2", "$ref": ""}
                        }
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
                            "security": [{"gssapi": []}],
                        })
                    }),
                    "components": IsPartialDict({
                        "securitySchemes": {"gssapi": {"type": "gssapi"}}
                    }),
                }),
                id="SASLGSSAPI",
            ),
        ),
    )
    def test_security_schema(
        self, security: BaseSecurity, schema: dict[str, str]
    ) -> None:
        broker = KafkaBroker(security=security)
        generated_schema = self.get_schema(broker)
        assert generated_schema.to_jsonable() == schema
