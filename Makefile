default: build

prerequisites:
	go get -u github.com/jteeuwen/go-bindata/...
	go get -u github.com/elazarl/go-bindata-assetfs/...
	go get -u bitbucket.org/tebeka/snowball/...

bindata:
	go-bindata-assetfs -pkg=cbft ./static/...
	go fmt bindata_assetfs.go

build: bindata
	go build -tags 'debug libstemmer' ./...
	go build -tags 'debug libstemmer' -o ./cbft ./cmd/cbft

build-leveldb: bindata
	go build -tags 'debug libstemmer leveldb' ./...
	go build -tags 'debug libstemmer leveldb' -o ./cbft ./cmd/cbft

build-forestdb: bindata
	go build -tags 'debug libstemmer forestdb' ./...
	go build -tags 'debug libstemmer forestdb' -o ./cbft ./cmd/cbft

build-full: bindata
	go build -tags 'debug libstemmer leveldb forestdb' ./...
	go build -tags 'debug libstemmer leveldb forestdb' -o ./cbft ./cmd/cbft

test:
	go test -v ./...

test-full:
	go test -v -tags 'debug leveldb forestdb' ./...

coverage:
	go test -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out
