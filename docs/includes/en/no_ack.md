!!! tip
    If you want to take over the acknowledgement logic completely, use `#!python @broker.subscriber(..., ack_policy=AckPolicy.MANUAL)`. **FastStream** then never acknowledges, rejects or nacks a message for you, so your handler has to call `msg.ack()`, `msg.nack()` or `msg.reject()` in every code path.
