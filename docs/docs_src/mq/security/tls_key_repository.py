from faststream.mq import MQBroker, mq_tls_from_keystore

broker = MQBroker(
    queue_manager="QM1",
    conn_name="localhost(1414)",
    tls=mq_tls_from_keystore(
        cipher_spec="TLS_AES_256_GCM_SHA384",
        keystore="docs/docs_src/mq/security/certs/client.p12",
        certificate_label="client-cert",
    ),
)
