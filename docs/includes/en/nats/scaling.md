If one `subject` is being listened to by several consumers with the same `queue group`, the message will go to a random consumer each time.

Thus, *NATS* can independently balance the load on queue consumers. You can increase the processing speed of the message flow from the queue by simply launching additional instances of the consumer service. You don't need to make changes to the current infrastructure configuration: *NATS* will take care of how to distribute messages between your services.

!!! tip
    By default, all subscribers are consuming messages from subject in blocking mode. You can't process multiple messages from the same subject in the same time. So, you have some kind of block per subject.

    But, all `NatsBroker` subscribers has `max_workers` argument allows you to consume messages in a per-subscriber pool. So, if you have subscriber like `#!python @broker.subscriber(..., max_workers=10)`, it means that you can process up to **10** by it in the same time.
