from dataclasses import dataclass
from pathlib import Path

from faststream.exceptions import SetupError


@dataclass(frozen=True)
class _BaseMQTLSConfig:
    cipher_spec: str
    peer_name: str | None = None
    certificate_label: str | None = None
    key_repo_password: str | None = None


@dataclass(frozen=True)
class MQPEMTLSConfig(_BaseMQTLSConfig):
    ca_chain_certs: str = ""
    client_cert_and_key: str = ""

    def validate(self) -> None:
        if not self.cipher_spec:
            raise SetupError("`cipher_spec` is required for IBM MQ TLS.")
        if not self.ca_chain_certs:
            raise SetupError(
                "`ca_chain_certs` is required for PEM-based IBM MQ TLS.",
            )
        if not self.client_cert_and_key:
            raise SetupError(
                "`client_cert_and_key` is required for PEM-based IBM MQ TLS.",
            )

        for name, value in {
            "ca_chain_certs": self.ca_chain_certs,
            "client_cert_and_key": self.client_cert_and_key,
        }.items():
            if not Path(value).exists():
                raise SetupError(f"`{name}` path does not exist: {value}")


@dataclass(frozen=True)
class MQKeyRepositoryTLSConfig(_BaseMQTLSConfig):
    key_repository: str = ""

    def validate(self) -> None:
        if not self.cipher_spec:
            raise SetupError("`cipher_spec` is required for IBM MQ TLS.")
        if not self.key_repository:
            raise SetupError(
                "`key_repository` is required for key-repository IBM MQ TLS.",
            )


MQTLSConfig = MQPEMTLSConfig | MQKeyRepositoryTLSConfig


def validate_tls_configuration(
    *,
    tls: MQTLSConfig | None,
    use_ssl: bool,
    ssl_context: object | None,
) -> None:
    if ssl_context is not None:
        raise SetupError(
            "`ssl_context` is not supported by IBM MQ. Use `tls=` with MQ-native TLS settings.",
        )

    if use_ssl and tls is None:
        raise SetupError(
            "IBM MQ TLS requires explicit `tls=` configuration. `security.use_ssl` alone is not enough.",
        )

    if tls is not None:
        tls.validate()
