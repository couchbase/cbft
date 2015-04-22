# Developer's guide

## Key concepts

### Index

An index or "logical index" defines a data system that supports fast
lookup of documents based on query criteria.

An often used analogy is that an index of a database is similar to a
book index.  A book index is comprised of alphabetically sorted
entries that allow readers to quickly locate relevant pages in the
book based on some search words.

Similarly, a cbft index allows users to quickly locate relevant
documents from a Couchbase bucket (for example), based on some search
terms or query criteria.

### Index Name

An index has a name, or _Index Name_, that is used as a unique
identifier for the index.  An index name is comprised of alphanumeric
characters, hyphens and underscores (no whitespace characters).

### Index Type

An index has a type, or _Index Type_.  An often used index type, for
example, would be "bleve", for full-text indexing.

### Source Type

An index has a _Source Type_, which specifies the kind of data-source
that is used to populate the index.  An often used source type, for
example, would be "couchbase", which would be used when a user wants
to index all the documents that are stored in a Couchbase bucket.

### Source Name and Source Params

An index also has _Source Name_ and optional _Source Params_.  The
meaning of the source name and source params depend on the source
type.  For example, when the source type is "couchbase", then the
source name is treated as a Couchbase bucket name, and the source
params would define any extra, additional parameters needed to connect
that named Couchbase bucket.

### Index Partition

At runtime, the entries maintained in a cbft index will be split into
one or more partitions, or _Index Partition_'s.

Index partitions will be dynamically assigned at runtime to one or
more cbft processes, or _Node_'s.  A user can deploy multiple nodes, via
cbft's clustering features, in order to increase performance and/or
availability via replication.

An index partition is sometimes abbreviated as "pindex" or "PIndex",
as you'll sometimes see in cbft's log files, stats entries and/or JSON
data.

An index partition is different than, but related to, the partitions
from a data source.  For example, Couchbase has a concept of
partitions of a bucket (a.k.a, "vbuckets").  One index partition might
be assigned to index all the data for a set of vbuckets.  Another
index partition might be assigned to index all the data for a different,
non-overlapping set of vbuckets.

### Node

A _Node_ is a cbft process.

A node has a unique _Node UUID_ and listens on a unique HTTP/REST IP
address and port, or _BindHttp_ parameter (host:port).

A node's UUID and host:port must be unique across a cbft cluster.

## Architecture

## Index Types

## Source Types



---

Copyright (c) 2015 Couchbase, Inc.
