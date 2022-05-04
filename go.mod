module github.com/couchbase/cbft

go 1.17

require (
	github.com/blevesearch/bleve-mapping-ui v0.4.0
	github.com/blevesearch/bleve/v2 v2.2.3-0.20220224151155-3c7d301db56a
	github.com/blevesearch/bleve_index_api v1.0.1
	github.com/blevesearch/upsidedown_store_api v1.0.1
	github.com/blevesearch/zapx/v11 v11.3.1
	github.com/blevesearch/zapx/v12 v12.3.1
	github.com/blevesearch/zapx/v13 v13.3.1
	github.com/blevesearch/zapx/v14 v14.3.1
	github.com/blevesearch/zapx/v15 v15.3.1
	github.com/buger/jsonparser v1.1.1
	github.com/couchbase/cbauth v0.1.1
	github.com/couchbase/cbftx v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/moss v0.3.0
	github.com/dustin/go-jsonpointer v0.0.0-20140810065344-75939f54b39e
	github.com/elazarl/go-bindata-assetfs v1.0.1
	github.com/golang/protobuf v1.4.0
	github.com/gorilla/mux v1.8.0
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede
	github.com/julienschmidt/httprouter v1.3.0
	github.com/spf13/cobra v0.0.5
	golang.org/x/net v0.0.0-20211020060615-d418f374d309
	golang.org/x/sys v0.0.0-20210423082822-04245dca01da
	google.golang.org/grpc v1.24.0
)

require (
	github.com/RoaringBitmap/roaring v0.9.4 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/mmap-go v1.0.3 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.1.0 // indirect
	github.com/blevesearch/segment v0.9.0 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/vellum v1.0.7 // indirect
	github.com/couchbase/blance v0.1.1 // indirect
	github.com/couchbase/ghistogram v0.1.0 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.8 // indirect
	github.com/couchbase/gomemcached v0.1.4 // indirect
	github.com/couchbase/hebrew v0.0.0-00010101000000-000000000000 // indirect
	github.com/dustin/gojson v0.0.0-20150115165335-af16e0e771e2 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/google/uuid v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.8.1-0.20180127015812-30136e27e2ac // indirect
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/steveyen/gtreap v0.1.0 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
	google.golang.org/protobuf v1.21.0 // indirect
)

replace golang.org/x/text => golang.org/x/text v0.3.7

replace github.com/couchbase/cbauth => ../goproj/src/github.com/couchbase/cbauth

replace github.com/couchbase/cbftx => ../cbftx

replace github.com/couchbase/hebrew => ../hebrew

replace github.com/couchbase/cbgt => ../cbgt

replace github.com/couchbase/cbft => ./empty

replace github.com/couchbase/go-couchbase => ../goproj/src/github.com/couchbase/go-couchbase

replace github.com/couchbase/gomemcached => ../goproj/src/github.com/couchbase/gomemcached

replace github.com/couchbase/goutils => ../goproj/src/github.com/couchbase/goutils
