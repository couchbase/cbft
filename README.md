cbft
====

Couchbase Full Text server

The cbft project integrates the bleve full-text search engine and Couchbase Server.

[![GoDoc](https://godoc.org/github.com/couchbase/cbft?status.svg)](https://godoc.org/github.com/couchbase/cbft) [![Coverage Status](https://coveralls.io/repos/couchbase/cbft/badge.png?branch=master)](https://coveralls.io/r/couchbase/cbft?branch=master)

A cbft process creates and maintains full-text indexes using the
[bleve full-text indexing engine](http://www.blevesearch.com/).

Data sources for indexing include Couchbase 3.0+ and Couchbase 4.0+ buckets.

Full-text indexes can be optionally partitioned across multiple cbft processes.

Queries on a cbft index will be scatter/gather'ed across the relevant, distributed cbft processes.

# See also

[Full-text search](https://www.couchbase.com/products/full-text-search)

# Licenses

* BSL 1.1
* See also: [third party licenses](https://github.com/couchbase/cbft/blob/master/LICENSE-thirdparty.txt)

# Getting started and documentation

Please see the [getting started](http://labs.couchbase.com/cbft) guide for cbft.

# For developers / contributors

Please see the [README for developers](https://github.com/couchbase/cbft/blob/master/README-dev.md)
