module github.com/couchbase/cbft

go 1.19

require (
	github.com/blevesearch/bleve-mapping-ui v0.4.0
	github.com/blevesearch/bleve/v2 v2.3.4-0.20240625181944-fdaed7be8e8a
	github.com/blevesearch/bleve_index_api v1.0.3-0.20240624205006-07f7b7930fd5
	github.com/blevesearch/upsidedown_store_api v1.0.1
	github.com/blevesearch/zapx/v11 v11.3.4
	github.com/blevesearch/zapx/v12 v12.3.4
	github.com/blevesearch/zapx/v13 v13.3.4
	github.com/blevesearch/zapx/v14 v14.3.4
	github.com/blevesearch/zapx/v15 v15.3.5-0.20230516194434-edc21be64631
	github.com/buger/jsonparser v1.1.1
	github.com/cloudfoundry/gosigar v1.3.4
	github.com/couchbase/cbauth v0.1.1
	github.com/couchbase/cbftx v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/moss v0.3.0
	github.com/dustin/go-jsonpointer v0.0.0-20140810065344-75939f54b39e
	github.com/elazarl/go-bindata-assetfs v1.0.1
	github.com/golang/protobuf v1.5.3
	github.com/gorilla/mux v1.8.0
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede
	github.com/julienschmidt/httprouter v1.3.0
	github.com/spf13/cobra v0.0.5
	golang.org/x/net v0.17.0
	golang.org/x/sys v0.13.0
	google.golang.org/grpc v1.58.3
)

require (
	github.com/RoaringBitmap/roaring v0.9.4 // indirect
	github.com/bits-and-blooms/bitset v1.2.2 // indirect
	github.com/blevesearch/geo v0.1.12-0.20220606102651-aab42add3121 // indirect
	github.com/blevesearch/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/goleveldb v1.0.1 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.0.4 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.1.0 // indirect
	github.com/blevesearch/segment v0.9.0 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/vellum v1.0.8 // indirect
	github.com/couchbase/blance v0.1.6 // indirect
	github.com/couchbase/ghistogram v0.1.0 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.11 // indirect
	github.com/couchbase/gomemcached v0.1.4 // indirect
	github.com/couchbase/hebrew v0.0.0-00010101000000-000000000000 // indirect
	github.com/dustin/gojson v0.0.0-20150115165335-af16e0e771e2 // indirect
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	golang.org/x/crypto v0.14.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230711160842-782d3b101e98 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

replace golang.org/x/text => golang.org/x/text v0.3.7

replace golang.org/x/crypto => golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa

replace github.com/couchbase/cbauth => ../goproj/src/github.com/couchbase/cbauth

replace github.com/couchbase/cbftx => ../cbftx

replace github.com/couchbase/hebrew => ../hebrew

replace github.com/couchbase/cbgt => ../cbgt

replace github.com/couchbase/cbft => ./empty

replace github.com/couchbase/go-couchbase => ../goproj/src/github.com/couchbase/go-couchbase

replace github.com/couchbase/gomemcached => ../goproj/src/github.com/couchbase/gomemcached

replace github.com/couchbase/goutils => ../goproj/src/github.com/couchbase/goutils
