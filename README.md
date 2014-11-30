cbft
====

Couchbase Full Text engine

This project integrates the bleve full-text search engine and Couchbase Server.

[![Build Status](https://drone.io/github.com/couchbaselabs/cbft/status.png)](https://drone.io/github.com/couchbaselabs/cbft/latest) [![Coverage Status](https://img.shields.io/coveralls/couchbaselabs/cbft.svg)](https://coveralls.io/r/couchbaselabs/cbft?branch=master)

LICENSE: Apache 2.0

### Usage

Getting

```go get -u github.com/couchbaselabs/cbft```

Running against local Couchbase Server

```./cbft -server http://localhost:8091 -wanted```

Create a new index (for the default bucket)

```curl -XPUT http://localhost:8095/api/index/default```

Check how many documents are indexed

```curl http://localhost:8095/api/index/default/count```

Submit search query

```curl -XPOST -d '{"query":{"query":{"query":"your-search-term"}}}' --header Content-Type:text/json http://localhost:9090/api/index/default-ft/query```

Delete index

```curl -XDELETE http://localhost:8095/api/index/default```

### For cbft developers

Getting

		mkdir -p $GOPATH/src/github.com/couchbaelabs
		cd $GOPATH/src/github.com/couchbaelabs
		git clone git://github.com/couchbaselabs/cbft.git

Building

```go build```

To get local coverage reports with heatmaps...

    go test -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out

To get more coverage reports that include dependencies like the bleve library...

    go test -coverpkg github.com/couchbaselabs/cbft,github.com/blevesearch/bleve,github.com/blevesearch/bleve/index -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out

