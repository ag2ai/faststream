---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis Streams

[Redis Streams](https://redis.io/docs/latest/develop/tools/insight/tutorials/insight-stream-consumer/){.external-link target="_blank"} are a data structure introduced in **Redis 5.0** that offer a reliable and highly scalable way to handle streams of data. They are similar to logging systems like **Apache Kafka**, where data is stored in a log structure and can be consumed by multiple clients. **Streams** provide a sequence of ordered messages, and they are designed to handle a high volume of data by allowing partitioning and multiple consumers.

A **Redis Stream** is a collection of entries, each having an ID (which includes a timestamp) and a set of key-value pairs representing the message data. Clients can add to a stream by generating a new entry and can read from a stream to consume its messages.

**Streams** have unique features such as:

- Persistence: Data in the stream are persisted and can be replayed by new consumers.
- Consumer Groups: Allow concurrent consumption and acknowledgment of data entries by multiple consumers, facilitating partitioned processing.
- Range Queries: Clients can query streams for data within a specific range of IDs.
