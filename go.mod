module github.com/couchbase/cbft

go 1.23

toolchain go1.23.7

require (
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.15.15
	github.com/aws/aws-sdk-go-v2/service/s3 v1.48.1
	github.com/blevesearch/bleve-mapping-ui v0.6.0
	github.com/blevesearch/bleve/v2 v2.4.4-0.20250320075922-5040e2d2092a
	github.com/blevesearch/bleve_index_api v1.2.3
	github.com/blevesearch/zapx/v11 v11.4.1
	github.com/blevesearch/zapx/v12 v12.4.1
	github.com/blevesearch/zapx/v13 v13.4.1
	github.com/blevesearch/zapx/v14 v14.4.1
	github.com/blevesearch/zapx/v15 v15.4.1
	github.com/blevesearch/zapx/v16 v16.2.2-0.20250305220028-89edb0ef9aa9
	github.com/buger/jsonparser v1.1.1
	github.com/cloudfoundry/gosigar v1.3.4
	github.com/couchbase/cbauth v0.1.13
	github.com/couchbase/cbftx v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/go_json v0.0.0-20220330123059-4473a21887c8
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/regulator v0.0.0-00010101000000-000000000000
	github.com/couchbase/tools-common/cloud/v5 v5.0.3
	github.com/couchbase/tools-common/types v1.1.4
	github.com/elazarl/go-bindata-assetfs v1.0.1
	github.com/golang/protobuf v1.5.4
	github.com/gorilla/mux v1.8.0
	github.com/json-iterator/go v1.1.12
	github.com/julienschmidt/httprouter v1.3.0
	github.com/spf13/cobra v1.8.1
	golang.org/x/net v0.24.0
	golang.org/x/sys v0.29.0
	golang.org/x/time v0.5.0
	google.golang.org/grpc v1.63.2
)

require (
	github.com/RoaringBitmap/roaring/v2 v2.4.5 // indirect
	github.com/aws/aws-sdk-go-v2 v1.30.3 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.2.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.5.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.2.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.10.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.2.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.10.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.16.10 // indirect
	github.com/aws/smithy-go v1.20.3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/blevesearch/geo v0.1.20 // indirect
	github.com/blevesearch/go-faiss v1.0.24 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/goleveldb v1.0.1 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.0.4 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.3.5 // indirect
	github.com/blevesearch/segment v0.9.1 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/stempel v0.2.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.2 // indirect
	github.com/blevesearch/vellum v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/couchbase/blance v0.1.6 // indirect
	github.com/couchbase/ghistogram v0.1.0 // indirect
	github.com/couchbase/gocb/v2 v2.9.4 // indirect
	github.com/couchbase/gocbcore/v10 v10.5.4 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.8 // indirect
	github.com/couchbase/gocbcoreps v0.1.3 // indirect
	github.com/couchbase/gomemcached v0.2.2-0.20230407174933-7d7ce13da8cc // indirect
	github.com/couchbase/goprotostellar v1.0.2 // indirect
	github.com/couchbase/hebrew v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/moss v0.3.0 // indirect
	github.com/couchbase/tools-common/fs v1.0.2 // indirect
	github.com/couchbase/tools-common/strings v1.0.0 // indirect
	github.com/couchbase/tools-common/sync/v2 v2.0.0 // indirect
	github.com/couchbase/tools-common/testing v1.0.1 // indirect
	github.com/couchbase/tools-common/utils/v3 v3.0.0 // indirect
	github.com/couchbaselabs/gocbconnstr/v2 v2.0.0-20240607131231-fb385523de28 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.12.2 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.34.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	go.etcd.io/bbolt v1.4.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/exp v0.0.0-20231226003508-02704c960a9b // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/json-iterator/go => github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede

replace golang.org/x/text => golang.org/x/text v0.4.0

replace golang.org/x/crypto => golang.org/x/crypto v0.19.0

replace github.com/couchbase/cbauth => ../goproj/src/github.com/couchbase/cbauth

replace github.com/couchbase/go_json => ../goproj/src/github.com/couchbase/go_json

replace github.com/couchbase/regulator => ../goproj/src/github.com/couchbase/regulator

replace github.com/couchbase/cbftx => ../cbftx

replace github.com/couchbase/hebrew => ../hebrew

replace github.com/couchbase/cbgt => ../cbgt

replace github.com/couchbase/cbft => ./empty

replace github.com/couchbase/go-couchbase => ../goproj/src/github.com/couchbase/go-couchbase

replace github.com/couchbase/gomemcached => ../goproj/src/github.com/couchbase/gomemcached

replace github.com/couchbase/goutils => ../goproj/src/github.com/couchbase/goutils
