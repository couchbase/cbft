# Key concepts

## Index

An index, or _Logical Index_, defines a data system that supports fast
lookup of documents based on query criteria.

An often used analogy is that an index of a database is similar to a
book index.  A book index is comprised of alphabetically sorted
entries that allow readers to quickly locate relevant pages in the
book based on some search words.

Similarly, a cbft index allows users to quickly locate relevant
documents from diferent data sources based on some search terms or
query criteria.

cbft supports multiple types of indexes, such as full-text indexes
and index aliases.

cbft also supports multiple kinds of data sources.

## Data Source

The data used to populate an index comes from a _Data Source_.

A data source, for example, might be all the documents stored in a
Couchbase bucket.

## Index Alias

An _Index Alias_ is a type of index that's not a normal index, but is
more of a virtual index that points to other, real indexes.

Similar to a symbolic link in a filesystem, an index alias allows a
naming level of indirection, so that applications can refer to a
stable name (the alias' name) while administrators can dynamically
retarget or re-point the index alias to different, real indexes.

Similar to an email list alias, too, an index alias in cbft can also
"fan-out" and refer to multiple, real indexes.  A query on an index
alias will scatter-gather the query request against all of the actual,
real indexes and merge results.

## Index Partition

At runtime, the data entries maintained in a cbft index will be split
into one or more partitions, or _Index Partition_'s.

An index partition is sometimes abbreviated as "pindex" or "PIndex",
as you'll sometimes see in cbft's log files, stats entries and/or JSON
data.

Put another way, a logical index is partitioned into one or more index
partitions.

## Source Partition

An index partition is different than, but related to, the partitions
from a data source, or _Source Partition_'s.

For example, Couchbase has a concept of partitions of a bucket (a.k.a,
"vbuckets").  So, a first index partition in cbft might be assigned to
ingest the data from some subset of source partitions or vbuckets.  A
second index partition in cbft might be assigned to ingest the data
from a different subset of source partitions or vbuckets.

## Node

Index partitions are an important part of cbft's design that allows
cbft to support a scale-out, distributed cluster of cbft processes, or
cbft _Node_'s.

Users can deploy multiple cbft nodes and cluster them together in
order to increase performance and/or increase availability via
replication.

Index partitions are dynamically assigned at runtime to one or more
cbft nodes, depending on replication policies and an index partition
assignment algorithm that attempts to achieve balanced workloads
across the cbft nodes in a cluster.

Each node has a unique _Node UUID_ and listens on a unique HTTP/REST
IP address and port (a.k.a, the _BindHttp_ command-line parameter on
node startup).

A node's UUID and HTTP/REST IP address and port must be unique across
a cbft cluster.

The nodes in a cbft cluster must also all use the same Cfg provider.

## Cfg

A _Cfg_ (or "config") provider is a configuration-oriented data system
required by cbft nodes.

A cbft node will store its configuration data into a Cfg provider.

For example, index definitions or metadata (but not large amounts of
index data entries) will be stored into a Cfg provider.

Some available Cfg providers...

- simple - for basic, non-clustered usage; the simple Cfg provider
  uses a local JSON file to store configuration data.  This is the
  default Cfg provider and is intended primarily to simplify the
  development-time experience and developer's usage of cbft.

- couchbase - uses a Couchbase bucket to store configuration data.  A
  couchbase Cfg provider is often used for clustering multiple cbft
  nodes, where all the cbft nodes need to be connected to the same
  Couchbase bucket for their configuration data.

---

Copyright (c) 2015 Couchbase, Inc.
