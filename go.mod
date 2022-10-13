module github.com/couchbase/cbft

go 1.18

require (
	github.com/blevesearch/bleve-mapping-ui v0.4.0
	github.com/blevesearch/bleve/v2 v2.0.4-0.20210810162943-2b21ae8f266f
	github.com/blevesearch/bleve_index_api v1.0.1
	github.com/blevesearch/upsidedown_store_api v1.0.1
	github.com/blevesearch/zapx/v11 v11.2.1-0.20210809173656-f061f2a21cb9
	github.com/blevesearch/zapx/v12 v12.2.1-0.20210809173531-2ea06c038419
	github.com/blevesearch/zapx/v13 v13.2.1-0.20210809173433-6a16986ce5d9
	github.com/blevesearch/zapx/v14 v14.2.1-0.20210809173320-a8a0c8c03c5b
	github.com/blevesearch/zapx/v15 v15.2.1-0.20210809172947-0534019802b1
	github.com/buger/jsonparser v1.1.1
	github.com/couchbase/cbauth v0.0.0-20200508215310-0d352b097b19
	github.com/couchbase/cbftx v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.0.0-20190523192451-b8e6d5d421bc
	github.com/couchbase/go-couchbase v0.0.0-20201026062457-7b3be89bbd89
	github.com/couchbase/goutils v0.0.0-20201030094643-5e82bb967e67
	github.com/couchbase/moss v0.1.0
	github.com/dustin/go-jsonpointer v0.0.0-20140810065344-75939f54b39e
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/golang/protobuf v1.4.0
	github.com/gorilla/mux v1.7.4
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede
	github.com/julienschmidt/httprouter v1.1.1-0.20170430222011-975b5c4c7c21
	github.com/spf13/cobra v0.0.5
	golang.org/x/net v0.0.0-20220728211354-c7608f3a8462
	golang.org/x/sys v0.0.0-20220728004956-3c1f35247d10
	google.golang.org/grpc v1.24.0
)

require (
	github.com/RoaringBitmap/roaring v0.4.23 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/mmap-go v1.0.2 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.0.1 // indirect
	github.com/blevesearch/segment v0.9.0 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/vellum v1.0.3 // indirect
	github.com/couchbase/blance v0.0.0-20210701151549-a83d808be6d1 // indirect
	github.com/couchbase/ghistogram v0.1.0 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.7-0.20210825200734-fa22caf5138a // indirect
	github.com/couchbase/gomemcached v0.0.0-20200618124739-5bac349aff71 // indirect
	github.com/dustin/gojson v0.0.0-20150115165335-af16e0e771e2 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/google/uuid v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.8.1-0.20180127015812-30136e27e2ac // indirect
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/steveyen/gtreap v0.1.0 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tinylib/msgp v1.1.0 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
	google.golang.org/protobuf v1.21.0 // indirect
)

replace github.com/couchbase/cbauth => ../goproj/src/github.com/couchbase/cbauth

replace github.com/couchbase/cbftx => ../cbftx

replace github.com/couchbase/cbgt => ../cbgt

replace github.com/couchbase/cbft => ./empty

replace github.com/couchbase/go-couchbase => ../goproj/src/github.com/couchbase/go-couchbase

replace github.com/couchbase/gomemcached => ../goproj/src/github.com/couchbase/gomemcached
