cbft
====

Couchbase Full Text server

The cbft project integrates the bleve full-text search engine and Couchbase Server.

[![Build Status](https://travis-ci.org/couchbaselabs/cbft.svg)](https://travis-ci.org/couchbaselabs/cbft) [![Coverage Status](https://coveralls.io/repos/couchbaselabs/cbft/badge.png?branch=master)](https://coveralls.io/r/couchbaselabs/cbft?branch=master) [![GoDoc](https://godoc.org/github.com/couchbaselabs/cbft?status.svg)](https://godoc.org/github.com/couchbaselabs/cbft)

A cbft process creates and maintains full-text indexes using the
[bleve full-text indexing engine](http://www.blevesearch.com/).

Data sources for indexing include Couchbase 3.0+ and Couchbase 4.0+
buckets.

Full-text indexes can be optionally partitioned across multiple cbft
processes.

Queries on a cbft index will be scatter/gather'ed across the relevant,
distributed cbft processes.

# Licenses

* Apache 2.0.
* See also: [third party licenses](https://github.com/couchbaselabs/cbft/blob/master/LICENSE-thirdparty.txt)

# Getting started and documentation

Please see the [getting started](http://labs.couchbase.com/cbft) guide
for cbft, available at
[http://labs.couchbase.com/cbft](http://labs.couchbase.com/cbft).

# For developers / contributors

Please see the [README for developers](https://github.com/couchbaselabs/cbft/blob/master/README-dev.md)
