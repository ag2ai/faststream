from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import TYPE_CHECKING
from secrets import token_hex

from faststream.exceptions import SetupError
from faststream.mq.tls import MQKeyRepositoryTLSConfig, MQPEMTLSConfig, MQTLSConfig

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class PreparedMQTLSConfig:
    cipher_spec: str
    key_repository: str
    certificate_label: str | None
    key_repo_password: str | None
    peer_name: str | None
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
    repo_base = tempdir / "mqclient"
    kdb_file = repo_base.with_suffix(".kdb")
    p12_file = tempdir / "client.p12"
    label = tls.certificate_label or "faststream-mq-client"
    password = tls.key_repo_password or token_hex(16)

    try:
        _run_tls_command(
            [
                "/opt/mqm/bin/runmqakm",
                "-keydb",
                "-create",
                "-db",
                str(kdb_file),
                "-pw",
                password,
                "-type",
                "cms",
                "-stash",
            ],
        )

        _run_tls_command(
            [
                "openssl",
                "pkcs12",
                "-export",
                "-in",
                tls.cert_file,
                "-inkey",
                tls.key_file,
                "-out",
                str(p12_file),
                "-name",
                label,
                "-passout",
                f"pass:{password}",
            ],
        )

        _run_tls_command(
            [
                "/opt/mqm/bin/runmqakm",
                "-cert",
                "-add",
                "-db",
                str(kdb_file),
                "-pw",
                password,
                "-label",
                "mq-ca",
                "-file",
                tls.ca_file,
                "-format",
                "ascii",
                "-trust",
                "enable",
            ],
        )

        _run_tls_command(
            [
                "/opt/mqm/bin/runmqakm",
                "-cert",
                "-import",
                "-target",
                str(kdb_file),
                "-target_pw",
                password,
                "-file",
                str(p12_file),
                "-type",
                "pkcs12",
                "-pw",
                password,
            ],
        )

    except Exception:
        shutil.rmtree(tempdir, ignore_errors=True)
        raise

    return PreparedMQTLSConfig(
        cipher_spec=tls.cipher_spec,
        key_repository=str(repo_base),
        certificate_label=label,
        key_repo_password=password,
        peer_name=tls.peer_name,
        tempdir=tempdir,
    )


def _run_tls_command(command: list[str]) -> None:
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except FileNotFoundError as e:
        raise SetupError(f"Required TLS tool is not available: {command[0]}") from e
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.strip()
        raise SetupError(f"MQ TLS setup command failed: {' '.join(command)}\n{stderr}") from e
