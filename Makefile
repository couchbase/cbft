default: build

bindata:
	# go get -u github.com/elazarl/go-bindata-assetfs/...
	go-bindata-assetfs -pkg=cbft ./static/...
	go fmt bindata_assetfs.go

build: bindata
	go build ./...
	go build -o ./cbft ./cmd/cbft

build-leveldb: bindata
	go build -tags 'debug leveldb' ./...
	go build -tags 'debug leveldb' -o ./cbft ./cmd/cbft

build-forestdb: bindata
	go build -tags 'debug forestdb' ./...
	go build -tags 'debug forestdb' -o ./cbft ./cmd/cbft

build-full: bindata
	go build -tags 'debug leveldb forestdb' ./...
	go build -tags 'debug leveldb forestdb' -o ./cbft ./cmd/cbft

test:
	go test -v ./...

test-full:
	go test -v -tags 'debug leveldb forestdb' ./...

coverage:
	go test -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out
