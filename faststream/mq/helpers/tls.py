from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import shutil
import tempfile
from secrets import token_hex

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import (
    PrivateFormat,
    load_pem_private_key,
    pkcs12,
)

from faststream.exceptions import SetupError
from faststream.mq.tls import MQKeyRepositoryTLSConfig, MQPEMTLSConfig, MQTLSConfig


@dataclass
class PreparedMQTLSConfig:
    cipher_spec: str
    key_repository: str
    certificate_label: str | None
    key_repo_password: str | None
    peer_name: str | None
    environment_scope: str | None = None
    tempdir: Path | None = None


def prepare_tls_config(tls: MQTLSConfig | None) -> PreparedMQTLSConfig | None:
    if tls is None:
        return None

    if isinstance(tls, MQKeyRepositoryTLSConfig):
        return PreparedMQTLSConfig(
            cipher_spec=tls.cipher_spec,
            key_repository=tls.key_repository,
            certificate_label=tls.certificate_label,
            key_repo_password=tls.key_repo_password,
            peer_name=tls.peer_name,
        )

    if isinstance(tls, MQPEMTLSConfig):
        return _prepare_pem_tls(tls)

    raise TypeError(f"Unsupported MQ TLS config: {type(tls)!r}")


def cleanup_prepared_tls(tls: PreparedMQTLSConfig | None) -> None:
    if tls is not None and tls.tempdir is not None:
        shutil.rmtree(tls.tempdir, ignore_errors=True)


def _prepare_pem_tls(tls: MQPEMTLSConfig) -> PreparedMQTLSConfig:
    tempdir = Path(tempfile.mkdtemp(prefix="faststream-mq-tls-"))
    p12_file = tempdir / "client.p12"
    label = tls.certificate_label or "faststream-mq-client"
    password = tls.key_repo_password or token_hex(16)

    try:
        p12_data = _build_pkcs12(
            client_cert_and_key=Path(tls.client_cert_and_key),
            ca_chain_certs=Path(tls.ca_chain_certs),
            certificate_label=label,
            password=password,
        )
        p12_file.write_bytes(p12_data)

    except Exception:
        shutil.rmtree(tempdir, ignore_errors=True)
        raise

    return PreparedMQTLSConfig(
        cipher_spec=tls.cipher_spec,
        key_repository=str(p12_file),
        certificate_label=label,
        key_repo_password=password,
        peer_name=tls.peer_name,
        environment_scope="CONNECTION",
        tempdir=tempdir,
    )


def _build_pkcs12(
    *,
    client_cert_and_key: Path,
    ca_chain_certs: Path,
    certificate_label: str,
    password: str,
) -> bytes:
    try:
        client_data = client_cert_and_key.read_bytes()
        key = load_pem_private_key(client_data, password=None)
        cert = _load_first_certificate(client_data)
        ca_certs = [
            pkcs12.PKCS12Certificate(ca_cert, f"ca-cert-{index}".encode("ascii"))
            for index, ca_cert in enumerate(
                _load_certificates(ca_chain_certs.read_bytes()),
                start=1,
            )
        ]

        encryption = (
            PrivateFormat.PKCS12.encryption_builder()
            .kdf_rounds(50000)
            .key_cert_algorithm(pkcs12.PBES.PBESv1SHA1And3KeyTripleDESCBC)
            .hmac_hash(hashes.SHA1())
            .build(password.encode("ascii"))
        )

        return pkcs12.serialize_key_and_certificates(
            certificate_label.encode("ascii"),
            key,
            cert,
            ca_certs,
            encryption,
        )
    except FileNotFoundError as e:
        raise SetupError(f"MQ TLS file not found: {e.filename}") from e
    except Exception as e:
        raise SetupError(f"MQ TLS setup failed: {e}") from e


def _load_first_certificate(data: bytes) -> x509.Certificate:
    certificates = _load_certificates(data)
    if not certificates:
        raise SetupError("No certificate found in `client_cert_and_key` PEM file.")
    return certificates[0]


def _load_certificates(data: bytes) -> list[x509.Certificate]:
    blocks = re.findall(
        rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        data,
        flags=re.DOTALL,
    )

    return [x509.load_pem_x509_certificate(block) for block in blocks]
