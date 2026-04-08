from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from secrets import token_hex
from typing import Iterable

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import (
    PrivateFormat,
    load_pem_private_key,
    pkcs12,
)

from faststream.exceptions import SetupError


@dataclass(frozen=True)
class MQTLSConfig:
    cipher_spec: str
    keystore: str
    peer_name: str | None = None
    certificate_label: str | None = None
    keystore_password: str | None = None
    _keystore_bytes: bytes | None = field(default=None, repr=False)
    _environment_scope: str | None = field(default="CONNECTION", repr=False)

    def __post_init__(self) -> None:
        if not self.cipher_spec:
            raise SetupError("`cipher_spec` is required for IBM MQ TLS.")
        if not self.keystore:
            raise SetupError("`keystore` is required for IBM MQ TLS.")
        if self._keystore_bytes is None and not Path(self.keystore).exists():
            raise SetupError(f"`keystore` path does not exist: {self.keystore}")


def mq_tls_from_keystore(
    *,
    cipher_spec: str,
    keystore: str,
    keystore_password: str | None = None,
    certificate_label: str | None = None,
    peer_name: str | None = None,
) -> MQTLSConfig:
    return MQTLSConfig(
        cipher_spec=cipher_spec,
        keystore=keystore,
        peer_name=peer_name,
        certificate_label=certificate_label,
        keystore_password=keystore_password,
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

    resolved_password = keystore_password or token_hex(32)

    return MQTLSConfig(
        cipher_spec=cipher_spec,
        keystore="<generated-pkcs12>",
        peer_name=peer_name,
        certificate_label=certificate_label,
        keystore_password=resolved_password,
        _keystore_bytes=_build_pkcs12(
            client_cert=Path(client_cert),
            client_key=Path(client_key),
            ca_certs=tuple(Path(x) for x in normalized_cas),
            certificate_label=certificate_label or "faststream-mq-client",
            keystore_password=resolved_password,
            client_key_password=client_key_password,
        ),
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


def _build_pkcs12(
    *,
    client_cert: Path,
    client_key: Path,
    ca_certs: tuple[Path, ...],
    certificate_label: str,
    keystore_password: str,
    client_key_password: str | None,
) -> bytes:
    try:
        cert_data = client_cert.read_bytes()
        key_data = client_key.read_bytes()
        cert = _load_first_certificate(cert_data, field_name="client_cert")
        key_password = (
            client_key_password.encode("utf-8") if client_key_password is not None else None
        )
        key = load_pem_private_key(key_data, password=key_password)

        ca_chain = _load_distinct_certificates(ca_certs)
        ca_entries = [
            pkcs12.PKCS12Certificate(ca_cert, f"ca-cert-{index}".encode("ascii"))
            for index, ca_cert in enumerate(ca_chain, start=1)
        ]

        encryption = (
            PrivateFormat.PKCS12.encryption_builder()
            .kdf_rounds(50000)
            .key_cert_algorithm(pkcs12.PBES.PBESv1SHA1And3KeyTripleDESCBC)
            .hmac_hash(hashes.SHA1())
            .build(keystore_password.encode("ascii"))
        )

        return pkcs12.serialize_key_and_certificates(
            certificate_label.encode("ascii"),
            key,
            cert,
            ca_entries,
            encryption,
        )

    except FileNotFoundError as e:
        raise SetupError(f"MQ TLS file not found: {e.filename}") from e
    except Exception as e:
        raise SetupError(f"MQ TLS setup failed: {e}") from e


def _load_distinct_certificates(paths: tuple[Path, ...]) -> list[x509.Certificate]:
    seen: set[bytes] = set()
    certs: list[x509.Certificate] = []

    for path in paths:
        data = path.read_bytes()
        for block in _extract_certificate_blocks(data):
            if block in seen:
                continue
            seen.add(block)
            certs.append(x509.load_pem_x509_certificate(block))

    return certs


def _load_first_certificate(data: bytes, *, field_name: str) -> x509.Certificate:
    blocks = _extract_certificate_blocks(data)
    if not blocks:
        raise SetupError(f"No certificate found in `{field_name}` PEM file.")
    return x509.load_pem_x509_certificate(blocks[0])


def _extract_certificate_blocks(data: bytes) -> list[bytes]:
    return re.findall(
        rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        data,
        flags=re.DOTALL,
    )
