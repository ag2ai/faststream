from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import shutil
import tempfile

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import (
    PrivateFormat,
    load_pem_private_key,
    pkcs12,
)

from faststream.exceptions import SetupError
from faststream.mq.tls import MQTLSConfig


@dataclass
class PreparedMQTLSConfig:
    cipher_spec: str
    key_repository: str
    keystore_password: str | None
    certificate_label: str | None
    peer_name: str | None
    environment_scope: str | None = None
    tempdir: Path | None = None


def prepare_tls_config(tls: MQTLSConfig | None) -> PreparedMQTLSConfig | None:
    if tls is None:
        return None

    if tls._key_repository is not None:
        return PreparedMQTLSConfig(
            cipher_spec=tls.cipher_spec,
            key_repository=tls._key_repository,
            keystore_password=tls.keystore_password,
            certificate_label=tls.certificate_label,
            peer_name=tls.peer_name,
            environment_scope=tls._environment_scope,
        )

    return _prepare_pem_tls(tls)


def cleanup_prepared_tls(tls: PreparedMQTLSConfig | None) -> None:
    if tls is not None and tls.tempdir is not None:
        shutil.rmtree(tls.tempdir, ignore_errors=True)


def _prepare_pem_tls(tls: MQTLSConfig) -> PreparedMQTLSConfig:
    tempdir = Path(tempfile.mkdtemp(prefix="faststream-mq-tls-"))
    p12_file = tempdir / "client.p12"

    try:
        p12_data = _build_pkcs12(
            client_cert=Path(tls._client_cert or ""),
            client_key=Path(tls._client_key or ""),
            ca_certs=tuple(Path(x) for x in tls._ca_certs),
            certificate_label=tls.certificate_label or "faststream-mq-client",
            keystore_password=tls.keystore_password or "",
            client_key_password=tls._client_key_password,
        )
        p12_file.write_bytes(p12_data)

    except Exception:
        shutil.rmtree(tempdir, ignore_errors=True)
        raise

    return PreparedMQTLSConfig(
        cipher_spec=tls.cipher_spec,
        key_repository=str(p12_file),
        keystore_password=tls.keystore_password,
        certificate_label=tls.certificate_label or "faststream-mq-client",
        peer_name=tls.peer_name,
        environment_scope=tls._environment_scope,
        tempdir=tempdir,
    )


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
