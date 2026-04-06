from faststream.mq import MQBroker, MQPEMTLSConfig

broker = MQBroker(
    queue_manager="QM1",
    conn_name="localhost(1414)",
    tls=MQPEMTLSConfig(
        cipher_spec="TLS_AES_256_GCM_SHA384",
        ca_chain_certs="docs/docs_src/mq/security/certs/ca.crt",
        client_cert_and_key="docs/docs_src/mq/security/certs/client.pem",
    ),
)
