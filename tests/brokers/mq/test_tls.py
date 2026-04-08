from pathlib import Path

import pytest

from faststream.exceptions import SetupError
from faststream.mq import MQBroker, mq_tls_from_keystore, mq_tls_from_pem
from faststream.mq.helpers.client import AsyncMQConnection, MQConnectionConfig
from faststream.security import BaseSecurity


@pytest.mark.mq()
def test_use_ssl_requires_explicit_tls() -> None:
    with pytest.raises(SetupError, match="explicit `tls=` configuration"):
        MQBroker(
            queue_manager="QM1",
            security=BaseSecurity(use_ssl=True),
        )


@pytest.mark.mq()
def test_ssl_context_is_not_supported_for_mq() -> None:
    with pytest.raises(SetupError, match="ssl_context"):
        MQBroker(
            queue_manager="QM1",
            security=BaseSecurity(use_ssl=True, ssl_context=object()),
            tls=mq_tls_from_keystore(
                cipher_spec="TLS_AES_256_GCM_SHA384",
                keystore=__file__,
            ),
        )


@pytest.mark.mq()
def test_key_repository_tls_connect_tcp_client(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeQueueManager:
        def connect_tcp_client(self, queue_manager, cd, channel, conn_name, user, password, cno=None, sco=None, bno=None):
            captured.update(
                {
                    "queue_manager": queue_manager,
                    "cd": cd,
                    "channel": channel,
                    "conn_name": conn_name,
                    "user": user,
                    "password": password,
                    "cno": cno,
                    "sco": sco,
                },
            )

    class FakeCNO:
        def __init__(self) -> None:
            self.Options = 0
            self.CCDTUrl = None

    class FakeCD:
        def __init__(self) -> None:
            self.SSLCipherSpec = b""
            self.SSLPeerNamePtr = 0
            self.DefReconnect = 0

    class FakeSCO:
        def __init__(self) -> None:
            self.KeyRepository = b""
            self.CertificateLabel = b""
            self.KeyRepoPassword = None

    class FakeMQ:
        QueueManager = lambda self=None: FakeQueueManager()
        CNO = FakeCNO
        CD = FakeCD
        SCO = FakeSCO

        class CMQC:
            MQCNO_RECONNECT_AS_DEF = 0
            MQCNO_RECONNECT = 1
            MQCNO_RECONNECT_DISABLED = 2
            MQCNO_RECONNECT_Q_MGR = 4

        class CMQXC:
            MQRCN_DISABLED = 0
            MQRCN_YES = 1
            MQRCN_Q_MGR = 2

    monkeypatch.setattr("faststream.mq.helpers.client._load_ibmmq", lambda: FakeMQ)

    connection = AsyncMQConnection(
        connection_config=MQConnectionConfig(
            queue_manager="QM1",
            channel="DEV.APP.SVRCONN",
            conn_name="host(1414)",
            username="app",
            password="password",
            tls=mq_tls_from_keystore(
                cipher_spec="TLS_AES_256_GCM_SHA384",
                peer_name="CN=qm1.example.com",
                keystore=__file__,
                certificate_label="client-cert",
                keystore_password="secret",
            ),
        ),
    )

    connection._connect_sync()

    cd = captured["cd"]
    sco = captured["sco"]
    assert captured["queue_manager"] == "QM1"
    assert captured["channel"] == "DEV.APP.SVRCONN"
    assert captured["conn_name"] == "host(1414)"
    assert captured["user"] == "app"
    assert captured["password"] == "password"
    assert cd.SSLCipherSpec == "TLS_AES_256_GCM_SHA384"
    assert cd.SSLPeerNamePtr == "CN=qm1.example.com"
    assert sco.KeyRepository == __file__
    assert sco.CertificateLabel == "client-cert"
    assert sco.KeyRepoPassword == "secret"


@pytest.mark.mq()
def test_pem_tls_connect_with_ccdt(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    client_pem = tmp_path / "client.pem"
    ca = tmp_path / "ca.crt"
    for path in (client_pem, ca):
        path.write_text("dummy")

    class FakeQueueManager:
        def connect_with_options(self, queue_manager, **kwargs) -> None:
            captured["queue_manager"] = queue_manager
            captured["kwargs"] = kwargs

    class FakeCNO:
        def __init__(self) -> None:
            self.Options = 0
            self.CCDTUrl = None

    class FakeCD:
        def __init__(self) -> None:
            self.SSLCipherSpec = b""
            self.SSLPeerNamePtr = 0

    class FakeSCO:
        def __init__(self) -> None:
            self.KeyRepository = b""
            self.CertificateLabel = b""
            self.KeyRepoPassword = None

    class FakeMQ:
        QueueManager = lambda self=None: FakeQueueManager()
        CNO = FakeCNO
        CD = FakeCD
        SCO = FakeSCO

        class CMQC:
            MQCNO_RECONNECT_AS_DEF = 0
            MQCNO_RECONNECT = 1
            MQCNO_RECONNECT_DISABLED = 2
            MQCNO_RECONNECT_Q_MGR = 4

        class CMQXC:
            MQRCN_DISABLED = 0
            MQRCN_YES = 1
            MQRCN_Q_MGR = 2

    monkeypatch.setattr("faststream.mq.helpers.client._load_ibmmq", lambda: FakeMQ)
    monkeypatch.setattr("faststream.mq.tls._build_pkcs12", lambda **kwargs: b"pkcs12-bytes")
    monkeypatch.setattr(
        "faststream.mq.helpers.client.prepare_tls_config",
        lambda tls: type(
            "Prepared",
            (),
                {
                    "cipher_spec": tls.cipher_spec,
                    "peer_name": tls.peer_name,
                    "key_repository": "/tmp/generated/keyrepo",
                    "certificate_label": "generated-cert",
                    "keystore_password": "generated-pass",
                    "environment_scope": "CONNECTION",
                    "tempdir": None,
                },
            )(),
        )

    connection = AsyncMQConnection(
        connection_config=MQConnectionConfig(
            queue_manager="QM1",
            ccdt_url="file:///tmp/AMQCLCHL.TAB",
            reconnect_mode="qmgr",
            tls=mq_tls_from_pem(
                cipher_spec="TLS_AES_256_GCM_SHA384",
                peer_name="CN=qm1.example.com",
                client_cert=str(client_pem),
                client_key=str(client_pem),
                ca_certs=(str(ca),),
            ),
        ),
    )

    connection._connect_sync()

    kwargs = captured["kwargs"]
    cd = kwargs["cd"]
    sco = kwargs["sco"]
    assert captured["queue_manager"] == "QM1"
    assert kwargs["cno"].CCDTUrl == "file:///tmp/AMQCLCHL.TAB"
    assert cd.SSLCipherSpec == "TLS_AES_256_GCM_SHA384"
    assert cd.SSLPeerNamePtr == "CN=qm1.example.com"
    assert sco.KeyRepository == "/tmp/generated/keyrepo"
    assert sco.CertificateLabel == "generated-cert"
    assert sco.KeyRepoPassword == "generated-pass"


@pytest.mark.mq()
def test_prepare_pem_tls_creates_pkcs12_store(monkeypatch, tmp_path: Path) -> None:
    from faststream.mq.helpers.tls import prepare_tls_config

    client_cert = tmp_path / "client.crt"
    client_key = tmp_path / "client.key"
    ca = tmp_path / "ca.crt"
    for path in (client_cert, client_key, ca):
        path.write_text("dummy")

    monkeypatch.setattr(
        "faststream.mq.tls._build_pkcs12",
        lambda **kwargs: b"pkcs12-bytes",
    )

    prepared = prepare_tls_config(
        mq_tls_from_pem(
            cipher_spec="TLS_AES_256_GCM_SHA384",
            client_cert=str(client_cert),
            client_key=str(client_key),
            ca_certs=(str(ca),),
            certificate_label="client-cert",
            keystore_password="secret",
        ),
    )

    assert prepared is not None
    assert prepared.key_repository.endswith("client.p12")
    assert prepared.certificate_label == "client-cert"
    assert Path(prepared.key_repository).read_bytes() == b"pkcs12-bytes"


@pytest.mark.mq()
def test_pem_tls_without_password_generates_strong_keystore_password(
    monkeypatch,
    tmp_path: Path,
) -> None:
    client_cert = tmp_path / "client.crt"
    client_key = tmp_path / "client.key"
    ca = tmp_path / "ca.crt"
    for path in (client_cert, client_key, ca):
        path.write_text("dummy")

    monkeypatch.setattr("faststream.mq.tls._build_pkcs12", lambda **kwargs: b"pkcs12")

    tls = mq_tls_from_pem(
        cipher_spec="TLS_AES_256_GCM_SHA384",
        client_cert=str(client_cert),
        client_key=str(client_key),
        ca_certs=(str(ca),),
    )

    assert tls.keystore_password is not None
    assert len(tls.keystore_password) == 64
