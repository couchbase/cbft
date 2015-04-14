CBFT_OUT  = ./cbft
CBFT_TAGS =

version = $(shell git describe --long)

goflags = \
    -ldflags '-X main.VERSION $(version)' \
    -tags "debug $(CBFT_TAGS)"

# -------------------------------------------------------------------
# Targets commonly used for day-to-day development...

default: build

gen-docs: cmd/cbft_docs/main.go
	go build -o ./cbft_docs ./cmd/cbft_docs
	./cbft_docs > docs/api-ref.md

gen-bindata:
	go-bindata-assetfs -pkg=cbft ./static/...
	go fmt bindata_assetfs.go

build: gen-bindata
	go build $(goflags) -o $(CBFT_OUT) ./cmd/cbft

build-leveldb:
	$(MAKE) build CBFT_TAGS="libstemmer leveldb"

build-forestdb:
	$(MAKE) build CBFT_TAGS="libstemmer forestdb"

build-full:
	$(MAKE) build CBFT_TAGS="libstemmer leveldb forestdb"

test:
	go test -v -tags "debug $(CBFT_TAGS)" .
	go test -v -tags "debug $(CBFT_TAGS)" ./cmd/cbft

test-full:
	$(MAKE) test CBFT_TAGS="leveldb forestdb"

coverage:
	go test -coverprofile=coverage.out -covermode=count
	go tool cover -html=coverage.out

# -------------------------------------------------------------------
# Release distribution related targets...

dist: test dist-meta dist-build

dist-meta:
	rm -rf ./dist/out
	mkdir -p ./dist/out
	mkdir -p ./static/dist
	echo $(version) > ./static/dist/version.txt
	cp ./static/dist/version.txt ./dist/out/version.txt
	./dist/go-manifest > ./static/dist/manifest.txt
	cp ./static/dist/manifest.txt ./dist/out/manifest.txt

dist-build:
	$(MAKE) gen-bindata gen-docs
	$(MAKE) build GOOS=darwin  GOARCH=amd64       CBFT_OUT=./dist/out/cbft.darwin.amd64
	$(MAKE) build GOOS=linux   GOARCH=386         CBFT_OUT=./dist/out/cbft.linux.386
	$(MAKE) build GOOS=linux   GOARCH=arm         CBFT_OUT=./dist/out/cbft.linux.arm
	$(MAKE) build GOOS=linux   GOARCH=arm GOARM=5 CBFT_OUT=./dist/out/cbft.linux.arm5
	$(MAKE) build GOOS=linux   GOARCH=amd64       CBFT_OUT=./dist/out/cbft.linux.amd64
	$(MAKE) build GOOS=freebsd GOARCH=amd64       CBFT_OUT=./dist/out/cbft.freebsd.amd64
	$(MAKE) build GOOS=windows GOARCH=386         CBFT_OUT=./dist/out/cbft.windows.386.exe
	$(MAKE) build GOOS=windows GOARCH=amd64       CBFT_OUT=./dist/out/cbft.windows.amd64.exe

dist-clean:
	rm -rf ./dist/out
	rm -rf ./static/dist/*
	git checkout bindata_assetfs.go

# -------------------------------------------------------------------
# The prereqs are for one time setup of required build/dist tools...

prereqs:
	go get github.com/jteeuwen/go-bindata/...
	go get github.com/elazarl/go-bindata-assetfs/...
	go get bitbucket.org/tebeka/snowball/...

prereqs-dist: prereqs
	go get golang.org/x/tools/cmd/cover/...
	go get golang.org/x/tools/cmd/vet/...
	pip install mkdocs
