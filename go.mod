module github.com/couchbase/cbft

go 1.13

require (
	github.com/blevesearch/bleve-mapping-ui v0.4.0
	github.com/blevesearch/bleve/v2 v2.0.2-0.20210208151822-36861ddf9f6d
	github.com/blevesearch/bleve_index_api v1.0.0
	github.com/blevesearch/upsidedown_store_api v1.0.1
	github.com/blevesearch/zapx/v11 v11.1.10
	github.com/blevesearch/zapx/v12 v12.1.10
	github.com/blevesearch/zapx/v13 v13.1.10
	github.com/blevesearch/zapx/v14 v14.1.10
	github.com/blevesearch/zapx/v15 v15.1.10
	github.com/buger/jsonparser v1.0.0
	github.com/couchbase/cbauth v0.0.0-20200508215310-0d352b097b19
	github.com/couchbase/cbftx v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.0.0-20190523192451-b8e6d5d421bc
	github.com/couchbase/go-couchbase v0.0.0-20201026062457-7b3be89bbd89
	github.com/couchbase/goutils v0.0.0-20201030094643-5e82bb967e67
	github.com/couchbase/moss v0.1.0
	github.com/dustin/go-jsonpointer v0.0.0-20140810065344-75939f54b39e
	github.com/dustin/gojson v0.0.0-20150115165335-af16e0e771e2 // indirect
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/golang/protobuf v1.4.0
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/gorilla/mux v1.7.4
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede
	github.com/julienschmidt/httprouter v1.1.1-0.20170430222011-975b5c4c7c21
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563
	github.com/spf13/cobra v0.0.5
	golang.org/x/net v0.0.0-20190311183353-d8887717615a
	golang.org/x/sys v0.0.0-20200212091648-12a6c2dcc1e4
	google.golang.org/grpc v1.24.0
)

replace github.com/couchbase/cbauth => ../goproj/src/github.com/couchbase/cbauth

replace github.com/couchbase/cbftx => ../cbftx

replace github.com/couchbase/cbgt => ../cbgt

replace github.com/couchbase/cbft => ./empty

replace github.com/couchbase/go-couchbase => ../goproj/src/github.com/couchbase/go-couchbase

replace github.com/couchbase/gomemcached => ../goproj/src/github.com/couchbase/gomemcached
