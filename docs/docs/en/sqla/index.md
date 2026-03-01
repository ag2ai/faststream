---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Motivation

The SQLA broker uses a relational database as a message broker.

## Trade-offs

The primary benefit of a message queue built on top of a relational database is the ability to insert messages **transactionally**, atomically with other database operations, thus enabling the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html){.external-link target="_blank"}. Also, the relational database is usually the most readily available, already-provisioned piece of infrastructure for a given service.

While implementing all patterns and semantics of a full-blown message queue or streaming platform (e.g. Kafka-like partition-enabled horizontal scaling with local ordering) would be problematic, given a proper understanding of the trade-offs involved, a relational-database-based queue is an appropriate tool for many low-to-medium throughput, latency-tolerant uses, including as part of a larger messaging flow that involves a "proper" queue (e.g. as an outbox between a service and a queue).
