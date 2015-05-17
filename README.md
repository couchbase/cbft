cbft
====

Couchbase Full Text server

This project integrates the bleve full-text search engine and Couchbase Server.

[![Build Status](https://drone.io/github.com/couchbaselabs/cbft/status.png)](https://drone.io/github.com/couchbaselabs/cbft/latest) [![Coverage Status](https://coveralls.io/repos/couchbaselabs/cbft/badge.png?branch=master)](https://coveralls.io/r/couchbaselabs/cbft?branch=master) [![GoDoc](https://godoc.org/github.com/couchbaselabs/cbft?status.svg)](https://godoc.org/github.com/couchbaselabs/cbft)

LICENSE: Apache 2.0

A cbft process creates and maintains full-text indexes using the
[bleve full-text indexing engine](http://www.blevesearch.com/).  Data
sources for indexing include Couchbase 3.0 buckets.  Full-text indexes
can be optionally partitioned across multiple cbft processes.  Queries
on an index will be scatter/gather'ed across the relevant, distributed
cbft processes.

# Getting started

Please see the [getting started](http://labs.couchbase.com/cbft)
guide for cbft.

# Documentation

Full [cbft documentation](http://labs.couchbase.com/cbft) is here...

* [http://labs.couchbase.com/cbft](http://labs.couchbase.com/cbft)

# For developers / contributors

Please see [the developer's README](https://github.com/couchbaselabs/cbft/blob/master/README-dev.md)
