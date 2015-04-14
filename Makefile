default: build

gen-bindata:
	go-bindata-assetfs -pkg=cbft ./static/...
	go fmt bindata_assetfs.go

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

# ----------------------------------------------------------------
# Build / release related targets...

gen-docs: cmd/cbft_docs/main.go
	go build -o ./cbft_docs ./cmd/cbft_docs
	./cbft_docs > docs/api-ref.md

gen-manifest:
	mkdir -p ./static/dist
	./dist/go-manifest > ./static/dist/manifest.txt

gen-version:
	mkdir -p ./static/dist
	git describe --long > ./static/dist/version.txt

dist: gen-version gen-manifest gen-docs
	mkdir -p ./dist/out
	./dist/dist.sh

dist-clean:
	rm -f ./static/manifest.txt
	rm -f ./static/version.txt
	rm -rf ./dist/out

# ----------------------------------------------------------------
# The prereqs are for one time setup of required tools...

prereqs:
	go get -u github.com/jteeuwen/go-bindata/...
	go get -u github.com/elazarl/go-bindata-assetfs/...
	go get -u bitbucket.org/tebeka/snowball/...

prereqs-docs:
	pip install mkdocs

prereqs-dist: prereqs prereqs-docs
	go get golang.org/x/tools/cmd/cover
	go get golang.org/x/tools/cmd/vet
