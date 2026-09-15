If one `subject` is being listened to by several consumers with the same `queue group`, the message will go to a random consumer each time.

Thus, *NATS* can independently balance the load on queue consumers. You can increase the processing speed of the message flow from the queue by simply launching additional instances of the consumer service. You don't need to make changes to the current infrastructure configuration: *NATS* will take care of how to distribute messages between your services.

!!! tip
    By default, all subscribers consume messages from a subject in blocking mode. You can't process multiple messages from the same subject at the same time, so you have some kind of lock per subject.

    But every `NatsBroker` subscriber has a `max_workers` argument that allows you to consume messages in a per-subscriber pool. So, if you have a subscriber like `#!python @broker.subscriber(..., max_workers=10)`, it can process up to **10** messages at the same time.
