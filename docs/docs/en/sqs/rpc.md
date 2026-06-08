# Request / response

SQS has no native request/reply, so FastStream implements RPC with a dedicated
**response queue** plus a `correlation_id`. Configure the response queue on the
broker, then call `broker.request`:

```python linenums="1"
{! docs_src/sqs/rpc/app.py [ln:3-17] !}
```

How it works:

1. The producer sends the request with `reply_to` set to the response queue URL
   and a generated `correlation_id`.
2. A background task long-polls the response queue, matching replies to pending
   requests by `correlation_id`, and deletes them once consumed.
3. The handler's return value is published back to `reply_to`.

!!! note ""
    `broker.request` raises `FeatureNotSupportedException` if no `response_queue`
    was configured.
