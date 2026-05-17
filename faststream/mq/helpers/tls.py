from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import tempfile
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

    if tls._keystore_bytes is None:
        return PreparedMQTLSConfig(
            cipher_spec=tls.cipher_spec,
            key_repository=tls.keystore,
            keystore_password=tls.keystore_password,
            certificate_label=tls.certificate_label,
            peer_name=tls.peer_name,
            environment_scope=tls._environment_scope,
        )

    tempdir = Path(tempfile.mkdtemp(prefix="faststream-mq-tls-"))
    p12_file = tempdir / "client.p12"
    p12_file.write_bytes(tls._keystore_bytes)

    return PreparedMQTLSConfig(
        cipher_spec=tls.cipher_spec,
        key_repository=str(p12_file),
        keystore_password=tls.keystore_password,
        certificate_label=tls.certificate_label,
        peer_name=tls.peer_name,
        environment_scope=tls._environment_scope,
        tempdir=tempdir,
    )


def cleanup_prepared_tls(tls: PreparedMQTLSConfig | None) -> None:
    if tls is not None and tls.tempdir is not None:
        shutil.rmtree(tls.tempdir, ignore_errors=True)
