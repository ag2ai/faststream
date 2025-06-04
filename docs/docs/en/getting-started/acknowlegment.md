# Acknowledgment

Since unexpected errors may occur during message processing, **FastStream** has an `ack_policy` parameter.

`AckPolicy` have 4 variants:

- `ACK` means that the message will be acked anyway.

- `NACK_ON_ERROR` means that the message will be nacked if an error occurs during processing and consumer will receive this message one more time.

- `REJECT_ON_ERROR` means that the message will be rejected if an error occurs during processing and consumer will not receive this message again.

- `DO_NOTHING` in this case *FastStream* will do nothing with the message. You must ack/nack/reject the message manually.


You must provide this parameter when initializing the subscriber.

```python linenums="1" hl_lines="5" title="main.py"
from faststream import AckPolicy
from faststream.nats import NatsBroker

broker = NatsBroker()
@broker.subscriber(ack_policy=AckPolicy.REJECT_ON_ERROR)
```
