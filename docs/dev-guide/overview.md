# Developer's guide overview

## Key concepts

### Index

An index or "logical index" defines a data system that supports fast
lookup of documents based on some matching criteria.  An often used
analogy would be that an index in a database is similar to an index in
a book.

A book index is comprised of a collection of alphabetically arranged
entries that allow readers to quickly locate relevant pages in the
book based on some indexed words.

Instead of a book, however, a cbft index instead allows users to
locate relevant documents from a Couchbase bucket (for example), based
on advanced search criteria (e.g., full-text search criteria).

### Index Name

An index has a unique name identifier, or Index Name, that is
comprised of alphanumeric characters, hyphens and underscores (no
whitespace characters).

### Index Type

An index has a type, or Index Type.  An often used index type, for
example, would be "bleve", for full-text indexing.

### Source Type

An index has a data source, which is defined by a Source Type.  An
often used source type would "couchbase", for example, which is used
when a user wants to index all the documents that are stored in a
Couchbase bucket.

### Source Name and Source Params

An index also has Source Name and optional Source Params.  The meaning
of the Source Name and Source Params depend on the Source Type.  For
example, when the Source Type is "couchbase", then the Source Name is
treated as a Couchbase bucket name, and the Source Params would define
any extra, additional parameters needed to connect that named
Couchbase bucket.

### Index Partition

At runtime, the entries maintained in a cbft index will be split into
one or more partitions, or Index Partitions.

Those index partitions will be dynamically assigned at runtime to one
or more cbft processes, or Nodes.  A user can deploy multiple nodes,
via cbft's clustering features, in order to increase performance
and/or availability via replication.

An index partition is sometimes abbreviated as "pindex" or "PIndex",
as you'll sometimes see in cbft's log files, stats and JSON data.

An index partition is different than, but related to, the partitions
from a data source.  For example, Couchbase has a concept of
partitions of a bucket (a.k.a, "vbuckets").  One index partition might
be assigned to index all the data for a set of vbuckets.  Another
index partition might be assigned to index all the data for a different,
non-overlapping set of vbuckets.

### Node

## Architecture

## Index Types

## Source Types



---

Copyright (c) 2015 Couchbase, Inc.
