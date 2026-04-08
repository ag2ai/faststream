from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from secrets import token_hex
from typing import Iterable

from faststream.exceptions import SetupError


@dataclass(frozen=True)
class MQTLSConfig:
    cipher_spec: str
    peer_name: str | None = None
    certificate_label: str | None = None
    keystore_password: str | None = None
    _key_repository: str | None = field(default=None, repr=False)
    _client_cert: str | None = field(default=None, repr=False)
    _client_key: str | None = field(default=None, repr=False)
    _ca_certs: tuple[str, ...] = field(default_factory=tuple, repr=False)
    _client_key_password: str | None = field(default=None, repr=False)
    _environment_scope: str | None = field(default="CONNECTION", repr=False)

    def __post_init__(self) -> None:
        if not self.cipher_spec:
            raise SetupError("`cipher_spec` is required for IBM MQ TLS.")

        has_keystore = self._key_repository is not None
        has_pem = all((self._client_cert, self._client_key, self._ca_certs))

        if has_keystore == has_pem:
            raise SetupError(
                "MQTLSConfig must describe either a keystore-based TLS setup or a PEM-based TLS setup.",
            )


def mq_tls_from_keystore(
    *,
    cipher_spec: str,
    keystore: str,
    keystore_password: str | None = None,
    certificate_label: str | None = None,
    peer_name: str | None = None,
) -> MQTLSConfig:
    if not keystore:
        raise SetupError("`keystore` is required for IBM MQ keystore TLS.")

    if not Path(keystore).exists():
        raise SetupError(f"`keystore` path does not exist: {keystore}")

    return MQTLSConfig(
        cipher_spec=cipher_spec,
        peer_name=peer_name,
        certificate_label=certificate_label,
        keystore_password=keystore_password,
        _key_repository=keystore,
    )


def mq_tls_from_pem(
    *,
    cipher_spec: str,
    client_cert: str,
    client_key: str,
    ca_certs: str | Iterable[str],
    keystore_password: str | None = None,
    certificate_label: str | None = None,
    client_key_password: str | None = None,
    peer_name: str | None = None,
) -> MQTLSConfig:
    if not client_cert:
        raise SetupError("`client_cert` is required for IBM MQ PEM TLS.")
    if not client_key:
        raise SetupError("`client_key` is required for IBM MQ PEM TLS.")

    normalized_cas = _normalize_ca_certs(ca_certs)

    for name, value in {
        "client_cert": client_cert,
        "client_key": client_key,
    }.items():
        if not Path(value).exists():
            raise SetupError(f"`{name}` path does not exist: {value}")

    for ca in normalized_cas:
        if not Path(ca).exists():
            raise SetupError(f"`ca_certs` path does not exist: {ca}")

    return MQTLSConfig(
        cipher_spec=cipher_spec,
        peer_name=peer_name,
        certificate_label=certificate_label,
        keystore_password=keystore_password or token_hex(32),
        _client_cert=client_cert,
        _client_key=client_key,
        _ca_certs=normalized_cas,
        _client_key_password=client_key_password,
    )


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


def _normalize_ca_certs(ca_certs: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(ca_certs, str):
        normalized = (ca_certs,)
    else:
        normalized = tuple(ca_certs)

    if not normalized:
        raise SetupError("`ca_certs` is required for IBM MQ PEM TLS.")

    return normalized
