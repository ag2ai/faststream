!!! tip
    If you want to take over the acknowledgement logic completely, use `#!python @broker.subscriber(..., ack_policy=AckPolicy.MANUAL)`. **FastStream** then never acknowledges, rejects or nacks a message for you, so your handler has to acknowledge, nack or reject every message itself, as the manual acknowledgement section above shows.
