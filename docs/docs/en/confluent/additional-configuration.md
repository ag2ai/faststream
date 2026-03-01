---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Passing Additional Configuration to confluent-kafka-python

The `confluent-kafka-python` package is a Python wrapper around [librdkakfa](https://github.com/confluentinc/librdkafka), which is a C/C++ client library for Apache Kafka.

`confluent-kafka-python` accepts a `config` dictionary that is then passed on to `librdkafka`. `librdkafka` provides plenty of [configuration properties](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) to configure the Kafka client.

**FastStream** also provides users with the ability to pass the config dictionary to `librdkafka` to provide greater customizability.

## Example

In the following example, we are setting the parameter `topic.metadata.refresh.fast.interval.ms`'s value to `300` instead of the default value `100` via the `config` parameter.

```python linenums="1" hl_lines="15 16"
{! docs_src/confluent/additional_config/app.py !}
```

Similarly, you could use the `config` parameter to pass any [configuration properties](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) to `librdkafka`.
