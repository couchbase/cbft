module github.com/couchbase/cbft

go 1.13

require (
	github.com/blevesearch/bleve v1.0.13-0.20201009151838-c67bc71f5a3f
	github.com/blevesearch/bleve-mapping-ui v0.3.0
	github.com/blevesearch/zap/v11 v11.0.12
	github.com/blevesearch/zap/v12 v12.0.12
	github.com/blevesearch/zap/v13 v13.0.4
	github.com/blevesearch/zap/v14 v14.0.3
	github.com/blevesearch/zap/v15 v15.0.1
	github.com/buger/jsonparser v1.0.0
	github.com/couchbase/cbauth v0.0.0-20200508215310-0d352b097b19
	github.com/couchbase/cbftx v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.0.0-20190523192451-b8e6d5d421bc
	github.com/couchbase/go-couchbase v0.0.0-20200618135536-fe11fe60aa9d
	github.com/couchbase/goutils v0.0.0-20191018232750-b49639060d85
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
	golang.org/x/net v0.0.0-20180906233101-161cd47e91fd
	golang.org/x/sys v0.0.0-20200212091648-12a6c2dcc1e4
	google.golang.org/genproto v0.0.0-20180831171423-11092d34479b // indirect
	google.golang.org/grpc v1.17.0
)

replace github.com/couchbase/cbftx => ../cbftx

replace github.com/couchbase/cbgt => ../cbgt

replace github.com/couchbase/cbft => ./empty
