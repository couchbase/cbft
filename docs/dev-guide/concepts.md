# Key concepts

## Index

An index or "logical index" defines a data system that supports fast
lookup of documents based on query criteria.

An often used analogy is that an index of a database is similar to a
book index.  A book index is comprised of alphabetically sorted
entries that allow readers to quickly locate relevant pages in the
book based on some search words.

Similarly, a cbft index allows users to quickly locate relevant
documents from a Couchbase bucket (for example), based on some search
terms or query criteria.

## Index Name

An index has a name, or _Index Name_, that is a unique identifier for
the index.  An index name is comprised of alphanumeric characters,
hyphens and underscores (no whitespace characters).

## Index Type

An index has a type, or _Index Type_.  An often used index type, for
example, would be "bleve", for full-text indexing.

Some available index types include...

- bleve - a full-text index powered by the
  [bleve](http://blevesearch.com) engine.

- blackhole - for testing; a blackhole index type ignores all incoming
  data, and returns errors on any queries.

- alias - an alias provides a naming level of indirection to one or
  more actual, target indexes; similar to a symbolic link in a
  filesystem or to an email mailing list group alias.

## Source Type

An index has a _Source Type_, which specifies the kind of data source
that is used to populate the index.  An often used source type, for
example, would be "couchbase", which would be used when a user wants
to index all the documents that are stored in a Couchbase bucket.

Some available source types include...

- couchbase - a Couchbase Server bucket will be the data source.
- nil - for testing; a nil data source never has any data.

## Source Name and Source Params

An index also has _Source Name_ and optional _Source Params_.  The
meaning of the source name and source params depend on the source
type.  For example, when the source type is "couchbase", then the
source name is treated as a Couchbase bucket name, and the source
params would define any extra, additional parameters needed to connect
that named Couchbase bucket.

## Index Partition

At runtime, the data maintained in a cbft index will be split into one
or more partitions, or _Index Partition_'s.

An index partition is sometimes abbreviated as "pindex" or "PIndex",
as you'll sometimes see in cbft's log files, stats entries and/or JSON
data.

## Source Partition

An index partition is different than, but related to, the partitions
from a data source, or _Source Partition_'s.

For example, Couchbase has a concept of partitions of a bucket (a.k.a,
"vbuckets").  So, a first index partition in cbft might be assigned to
ingest the data from some subset of source partitions / vbuckets.
A second index partition in cbft might be assigned to ingest the data
from a different subset of source partitions / vbuckets.

## Node

Index partitions are an important part of cbft's design that allows
cbft to support a scale-out, distributed cluster of cbft processes, or
cbft _Node_'s.

Users can deploy multiple cbft nodes and cluster them together in
order to increase performance and/or availability via replication.

Index partitions are dynamically assigned at runtime to one or more
cbft nodes, depending on replication policies and an index partition
assignment algorithm that attempts to achieve balanced workloads
across the cbft nodes in a cluster.

Each node has a unique _Node UUID_ and listens on a unique HTTP/REST
IP address and port (a.k.a, the _BindHttp_ command-line parameter on
node startup).

A node's UUID and HTTP/REST IP address and port must be unique across
a cbft cluster.

The nodes in a cbft cluster must all have the same Cfg provider.

## Cfg

A _Cfg_ (or "config") provider is a configuration-oriented data system
required by cbft nodes.

A cbft node will store its configuration data into a Cfg provider.

For example, index definitions (but not actual index data entries)
will be stored into a Cfg provider.

Some available Cfg providers...

- simple - for basic, non-clustered usage; the simple Cfg provider
  uses a local JSON file to store configuration data.  This is the
  default Cfg provider and is intended primarily to simplify the
  development-time experience and usage of cbft.

- couchbase - uses a Couchbase bucket to store configuration data.  A
  couchbase Cfg provider is often used for clustering multiple cbft
  nodes, where all the cbft nodes in a cluster need to be connected to
  the same Couchbase bucket for their Cfg.

---

Copyright (c) 2015 Couchbase, Inc.
