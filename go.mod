module github.com/couchbase/cbft

go 1.13

require (
	github.com/blevesearch/bleve v1.0.9
	github.com/blevesearch/bleve-mapping-ui v0.3.0
	github.com/couchbase/cbauth v0.0.0-20190926094514-82614adbe4d4
	github.com/couchbase/cbftx v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.0.0-20190523192451-b8e6d5d421bc
	github.com/couchbase/go-couchbase v0.0.0-20200406141124-ccd08288787d
	github.com/couchbase/gomemcached v0.0.0-20200526233749-ec430f949808 // indirect
	github.com/couchbase/goutils v0.0.0-20191018232750-b49639060d85
	github.com/couchbase/moss v0.1.0
	github.com/dustin/go-jsonpointer v0.0.0-20160814072949-ba0abeacc3dc
	github.com/dustin/gojson v0.0.0-20160307161227-2e71ec9dd5ad // indirect
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/golang/protobuf v1.4.0
	github.com/gorilla/mux v1.7.4
	github.com/json-iterator/go v1.1.9
	github.com/julienschmidt/httprouter v1.3.0
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	golang.org/x/net v0.0.0-20190404232315-eb5bcb51f2a3
	golang.org/x/sys v0.0.0-20200202164722-d101bd2416d5
	google.golang.org/grpc v1.29.0
)

replace github.com/couchbase/cbftx => ../cbftx

replace github.com/couchbase/cbgt => ../cbgt

replace github.com/couchbase/cbft => ../cbft
