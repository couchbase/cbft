default: build

prerequisites:
	go get -u github.com/jteeuwen/go-bindata/...
	go get -u github.com/elazarl/go-bindata-assetfs/...
	go get -u bitbucket.org/tebeka/snowball/...

gen-bindata:
	go-bindata-assetfs -pkg=cbft ./static/...
	go fmt bindata_assetfs.go

gen-docs: cmd/cbft_docs/main.go
	go build -o ./cbft_docs ./cmd/cbft_docs
	./cbft_docs > docs/api-guide.md

build: gen-bindata
	go build -tags 'debug libstemmer' ./...
	go build -tags 'debug libstemmer' -o ./cbft ./cmd/cbft

build-leveldb: gen-bindata
	go build -tags 'debug libstemmer leveldb' ./...
	go build -tags 'debug libstemmer leveldb' -o ./cbft ./cmd/cbft

build-forestdb: gen-bindata
	go build -tags 'debug libstemmer forestdb' ./...
	go build -tags 'debug libstemmer forestdb' -o ./cbft ./cmd/cbft

build-full: gen-bindata
	go build -tags 'debug libstemmer leveldb forestdb' ./...
	go build -tags 'debug libstemmer leveldb forestdb' -o ./cbft ./cmd/cbft

test:
	go test -v .
	go test -v ./cmd/cbft

test-full:
	go test -v -tags 'debug leveldb forestdb' .
	go test -v -tags 'debug leveldb forestdb' ./cmd/cbft

coverage:
	go test -coverprofile=coverage.out -covermode=count
	go tool cover -html=coverage.out

