cbft
====

Couchbase Full Text engine

This project integrates the bleve full-text search engine and Couchbase Server.

LICENSE: Apache 2.0

### Usage

Running against local Couchbase Server

```./cbft -server http://localhost:8091```

Create a new index (for a bucket that already exists)

```curl -XPUT http://localhost:8095/api/default```

Check how many documents are indexed

``` curl http://localhost:8095/api/default/_count```

Submit search query

```  curl -XPOST http://localhost:8095/api/default/_search -d curl -XPOST http://localhost:8095/api/default/_search -d '{"query": {"query":"searchterm"}}'```

Delete index

```curl -XDELETE http://localhost:8095/api/default```



### Status

[![Build Status](https://drone.io/github.com/couchbaselabs/cbft/status.png)](https://drone.io/github.com/couchbaselabs/cbft/latest)

[![Coverage Status](https://img.shields.io/coveralls/couchbaselabs/cbft.svg)](https://coveralls.io/r/couchbaselabs/cbft?branch=master)