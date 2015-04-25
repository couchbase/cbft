# Increasing performance

There are several aspects to performance, often with competing tradeoffs:

- indexing performance, or ingest performance - the speed at which the
  indexing system can incorporate data from the data source

- query performance - how fast the system responds to a query

There a several orthogonal dimensions, too...

- latency

- throughput

And, several ways to address the above aspects...

- more cpu
- more memory
- faster storage (e.g., SSD's)
- more or fewer nodes
- smaller indexes
- tighter queries (smaller result sets)

Distributing an index across multiple cbft nodes can increase indexing
speed but potentially at the cost or tradeoff of slower query speeds
(more scatter/gather work is needed).

tbd - more here

---

Copyright (c) 2015 Couchbase, Inc.
