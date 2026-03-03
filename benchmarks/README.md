
##  How to run
- Make sure that the broker is running and available.

- Run the command from the benchmarks dir.

```docker
docker run --rm \
  --cpus="0.5" \
  --memory="256m" \
  -v "$PWD":/benchmarks \
  -w /benchmarks \
  --network=host \
  python:3.12-slim \
  /bin/bash -c "
  pip install 'faststream[rabbit,redis,nats,kafka,confluent]==0.6.0rc0' fast-depends psutil pytest && \
  python bench.py"
```
