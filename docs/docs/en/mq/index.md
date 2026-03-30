---
search:
  boost: 10
---

# IBM MQ

**FastStream** IBM MQ support is implemented on top of the [`ibmmq`](https://github.com/ibm-messaging/mq-mqi-python){.external-link target="_blank"} client.

## What IBM MQ support covers

- queue-based publish and subscribe
- blocking request/reply using temporary reply queues
- manual acknowledgement via MQ transaction control
- FastAPI integration, AsyncAPI generation, Prometheus, and OpenTelemetry middleware

## Basic connection model

IBM MQ support in **FastStream** is queue-first.

```python
from faststream import FastStream
from faststream.mq import MQBroker

broker = MQBroker(
    queue_manager="QM1",
    conn_name="localhost(1414)",
    username="app",
    password="password",
)
app = FastStream(broker)
```

## Notes

- queues must already exist in normal runtime usage
- TLS/client certificate support is planned separately and is not part of the current IBM MQ integration
