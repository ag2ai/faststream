from faststream.mq import MQBroker, MQKeyRepositoryTLSConfig

broker = MQBroker(
    queue_manager="QM1",
    conn_name="localhost(1414)",
    tls=MQKeyRepositoryTLSConfig(
        cipher_spec="TLS_AES_256_GCM_SHA384",
        key_repository="/var/mq/keyrepo/client",
        certificate_label="client-cert",
    ),
)
