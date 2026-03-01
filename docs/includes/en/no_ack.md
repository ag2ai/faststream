!!! tip
    If you want to disable **FastStream** Acknowledgement logic at all, you can use
    `#!python @broker.subscriber(..., no_ack=True)` option. This way you should always process a message (ack/nack/terminate/etc) by yourself.
