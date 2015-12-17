cbft
====

Couchbase Full Text server

The cbft project integrates the bleve full-text search engine and Couchbase Server.

[![GoDoc](https://godoc.org/github.com/couchbase/cbft?status.svg)](https://godoc.org/github.com/couchbase/cbft) [![Build Status](https://travis-ci.org/couchbase/cbft.svg)](https://travis-ci.org/couchbase/cbft) [![Coverage Status](https://coveralls.io/repos/couchbase/cbft/badge.png?branch=master)](https://coveralls.io/r/couchbase/cbft?branch=master)

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
* See also: [third party licenses](https://github.com/couchbase/cbft/blob/master/LICENSE-thirdparty.txt)

# Getting started and documentation

Please see the [getting started](http://labs.couchbase.com/cbft) guide
for cbft, available at
[http://labs.couchbase.com/cbft](http://labs.couchbase.com/cbft).

# For developers / contributors

Please see the [README for developers](https://github.com/couchbase/cbft/blob/master/README-dev.md)

## For Couchbase Server developers

For running using Couchbase Server's cluster_run (in watson and beyond)...

    cd (your dev directory where you ran 'repo sync' for couchbase server watson.xml)
    make -j 9
    mkdir -p install/lib/fts
    rm -rf install/lib/fts/static
    rm -rf install/lib/fts/static-bleve-mapping
    rm -rf install/lib/fts/staticx
    ln -s ../../../goproj/src/github.com/couchbase/cbft/staticx/ install/lib/fts
    ln -s ../../../goproj/src/github.com/couchbase/cbgt/rest/static/ install/lib/fts
    ln -s ../../../godeps/src/github.com/blevesearch/bleve-mapping-ui/static-bleve-mapping/ install/lib/fts
    cp goproj/src/github.com/couchbase/cbft/ns_server_static/fts/* install/lib/fts
    cd ns_server
    ./cluster_run --pluggable-config="../install/etc/couchbase/pluggable-ui-fts.json"
