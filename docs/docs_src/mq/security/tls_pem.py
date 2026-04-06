from faststream.mq import MQBroker, MQPEMTLSConfig

broker = MQBroker(
    queue_manager="QM1",
    conn_name="localhost(1414)",
    tls=MQPEMTLSConfig(
        cipher_spec="TLS_AES_256_GCM_SHA384",
        cert_file="docs/docs_src/mq/security/certs/client.crt",
        key_file="docs/docs_src/mq/security/certs/client.key",
        ca_file="docs/docs_src/mq/security/certs/ca.crt",
    ),
)
