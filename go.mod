module github.com/couchbase/cbft

go 1.13

require (
	github.com/blevesearch/bleve-mapping-ui v0.4.0
	github.com/blevesearch/bleve/v2 v2.2.2
	github.com/blevesearch/bleve_index_api v1.0.1
	github.com/blevesearch/upsidedown_store_api v1.0.1
	github.com/blevesearch/zapx/v11 v11.3.1
	github.com/blevesearch/zapx/v12 v12.3.1
	github.com/blevesearch/zapx/v13 v13.3.1
	github.com/blevesearch/zapx/v14 v14.3.1
	github.com/blevesearch/zapx/v15 v15.3.1
	github.com/buger/jsonparser v1.1.1
	github.com/couchbase/cbauth v0.1.0
	github.com/couchbase/cbftx v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/moss v0.1.0
	github.com/dustin/go-jsonpointer v0.0.0-20140810065344-75939f54b39e
	github.com/dustin/gojson v0.0.0-20150115165335-af16e0e771e2 // indirect
	github.com/elazarl/go-bindata-assetfs v1.0.1
	github.com/golang/protobuf v1.4.0
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede
	github.com/julienschmidt/httprouter v1.3.0
	github.com/spf13/cobra v0.0.5
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	golang.org/x/sys v0.0.0-20200323222414-85ca7c5b95cd
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/grpc v1.24.0
)

replace github.com/couchbase/cbauth => ../goproj/src/github.com/couchbase/cbauth

replace github.com/couchbase/cbftx => ../cbftx

replace github.com/couchbase/cbgt => ../cbgt

replace github.com/couchbase/cbft => ./empty

replace github.com/couchbase/go-couchbase => ../goproj/src/github.com/couchbase/go-couchbase

replace github.com/couchbase/gomemcached => ../goproj/src/github.com/couchbase/gomemcached

replace github.com/couchbase/goutils => ../goproj/src/github.com/couchbase/goutils
