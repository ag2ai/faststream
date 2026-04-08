from faststream.mq import MQBroker, mq_tls_from_pem

broker = MQBroker(
    queue_manager="QM1",
    conn_name="localhost(1414)",
    tls=mq_tls_from_pem(
        cipher_spec="TLS_AES_256_GCM_SHA384",
        client_cert="docs/docs_src/mq/security/certs/client.crt",
        client_key="docs/docs_src/mq/security/certs/client.key",
        ca_certs=("docs/docs_src/mq/security/certs/ca.crt",),
    ),
)
