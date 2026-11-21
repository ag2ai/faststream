# Test troubleshooting

## Kafka connected tests

Check broker readiness and recent logs before changing the test environment:

```bash
docker compose logs --tail=100 kafka
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

- After startup, wait until the topic listing succeeds and rerun the failing test. If the logs show a JVM heap failure, save the logs and restart the Kafka service.
- If the broker answers inside its container but the test cannot connect, check `localhost:9092` from the environment running pytest. The dev container uses host networking. On Docker Desktop, enable host networking or run the test on the host.
- If a message published immediately after `start()` goes missing, check the consumer's offset policy. `get_subscriber_params` sets `earliest` because a consumer using `latest` can resolve its starting offset after the publish.
- If a failure follows a reused topic or consumer group, reproduce it with a fresh name. Reset the affected broker’s storage only after confirming that persisted state causes the failure and that its test data can be discarded.

## Resource warnings

Garbage collection can report a leaked resource during a later test. Rerun the suspected tests with a local pytest plugin loaded via `-p` that calls `gc.collect()` after each test call. If the warning lacks an allocation traceback, set `PYTHONTRACEMALLOC=1` and rerun the suspected test files to locate the allocation. CLI subprocesses inherit the setting and can time out under tracing.
