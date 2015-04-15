CBFT_CHECKOUT = origin/master
CBFT_DOCKER   = cbft-builder:latest
CBFT_OUT      = ./cbft
CBFT_TAGS     =

pwd     = $(shell pwd)
version = $(shell git describe --long)
goflags = -ldflags '-X main.VERSION $(version)' \
          -tags "debug $(CBFT_TAGS)"

# -------------------------------------------------------------------
# Targets commonly used for day-to-day development...

default: build

clean:
	rm -f ./cbft ./cbft_docs

build: gen-bindata
	go build $(goflags) -o $(CBFT_OUT) ./cmd/cbft

build-forestdb:
	$(MAKE) build CBFT_TAGS="icu libstemmer kagome forestdb"

build-leveldb:
	$(MAKE) build CBFT_TAGS="icu libstemmer kagome leveldb"

build-full:
	$(MAKE) build CBFT_TAGS="icu libstemmer kagome forestdb leveldb"

gen-bindata:
	go-bindata-assetfs -pkg=cbft ./static/...
	go fmt bindata_assetfs.go

gen-docs: cmd/cbft_docs/main.go
	go build -o ./cbft_docs ./cmd/cbft_docs
	./cbft_docs > docs/api-ref.md

test:
	go test -v -tags "debug $(CBFT_TAGS)" .
	go test -v -tags "debug $(CBFT_TAGS)" ./cmd/cbft

test-full:
	$(MAKE) test CBFT_TAGS="icu libstemmer kagome forestdb leveldb"

coverage:
	go test -coverprofile=coverage.out -covermode=count
	go tool cover -html=coverage.out

# -------------------------------------------------------------------
# Release / distribution related targets...

dist: test dist-meta dist-build

dist-meta:
	mkdir -p ./dist/out
	mkdir -p ./static/dist
	rm -rf ./dist/out/*
	rm -rf ./static/dist/*
	echo $(version) > ./static/dist/version.txt
	cp ./static/dist/version.txt ./dist/out/version.txt
	./dist/go-manifest > ./static/dist/manifest.txt
	cp ./static/dist/manifest.txt ./dist/out/manifest.txt

dist-build:
	$(MAKE) build         GOOS=darwin  GOARCH=amd64       CBFT_OUT=./dist/out/cbft.darwin.amd64
	$(MAKE) build         GOOS=linux   GOARCH=386         CBFT_OUT=./dist/out/cbft.linux.386
	$(MAKE) build         GOOS=linux   GOARCH=arm         CBFT_OUT=./dist/out/cbft.linux.arm
	$(MAKE) build         GOOS=linux   GOARCH=arm GOARM=5 CBFT_OUT=./dist/out/cbft.linux.arm5
	$(MAKE) build-leveldb GOOS=linux   GOARCH=amd64       CBFT_OUT=./dist/out/cbft.linux.amd64
	$(MAKE) build         GOOS=freebsd GOARCH=amd64       CBFT_OUT=./dist/out/cbft.freebsd.amd64
	$(MAKE) build         GOOS=windows GOARCH=386         CBFT_OUT=./dist/out/cbft.windows.386.exe
	$(MAKE) build         GOOS=windows GOARCH=amd64       CBFT_OUT=./dist/out/cbft.windows.amd64.exe

dist-clean: clean
	rm -rf ./dist/out/*
	rm -rf ./static/dist/*
	git checkout bindata_assetfs.go

# The release target requires...
#
#   export GITHUB_TOKEN=/* a github access token */
#   export GITHUB_USER=couchbaselabs
#
# See: https://help.github.com/articles/creating-an-access-token-for-command-line-use
#
# To release a new version...
#
#   git grep v0.0.1 # Look for old version strings.
#   <edit/update files, like cmd/cbft/main.go>
#   make test
#   <and, more tests, etc>
#   git commit -m "v0.0.2"
#   git tag -a "v0.0.2" -m "v0.0.2"
#   git push --tags
#   make release
#
# Remember, we use semver versioning rules.
#
# Of note, the version.go/VERSION is only updated on data/config format changes.
#
release: release-build release-push

release-build:
	mkdir -p $(pwd)/tmp/dist-out
	mkdir -p $(pwd)/tmp/dist-site
	rm -rf $(pwd)/tmp/dist-out/*
	rm -rf $(pwd)/tmp/dist-site/*
	docker run --rm \
		-v $(pwd)/tmp/dist-out:/tmp/dist-out \
		-v $(pwd)/tmp/dist-site:/tmp/dist-site \
		$(CBFT_DOCKER) \
		make -C /go/src/github.com/couchbaselabs/cbft \
			CBFT_CHECKOUT=$(CBFT_CHECKOUT) \
			release-build-helper dist-clean
	$(foreach FILE,$(wildcard $(pwd)/tmp/dist-out/cbft.*.exe),\
		zip $(FILE).zip $(FILE);)
	$(foreach FILE,$(wildcard $(pwd)/tmp/dist-out/cbft.*.amd64),\
		tar -zcvf $(FILE).tar.gz $(FILE);)
	rm -rf ./site/*
	cp -R $(pwd)/tmp/dist-site/* ./site

release-build-helper: # This runs inside a cbft-builder docker container.
	git remote update
	git fetch --tags
	git checkout $(CBFT_CHECKOUT)
	$(MAKE) dist
	$(MAKE) gen-docs
	mkdocs build --clean
	mkdir -p /tmp/dist-out
	mkdir -p /tmp/dist-site
	rm -rf /tmp/dist-out/*
	rm -rf /tmp/dist-site/*
	cp -R ./dist/out/* /tmp/dist-out
	cp -R ./site/* /tmp/dist-site

release-push:
	$(GOPATH)/bin/github-release --verbose release \
		--repo cbft \
		--tag $(strip $(shell git describe --abbrev=0 --tags)) \
		--pre-release || true
	$(foreach FILE,$(wildcard ./tmp/dist-out/*.gz ./tmp/dist-out/*.zip),\
		$(GOPATH)/bin/github-release upload \
			--repo cbft \
			--tag $(strip $(shell git describe --abbrev=0 --tags)) \
			--name $(strip $(shell cat ./tmp/dist-out/version.txt))_$(notdir $(FILE)) \
			--file $(FILE);)
	mkdocs gh-deploy

# -------------------------------------------------------------------
# The prereqs are for one time setup of required build/dist tools...

prereqs:
	go get github.com/jteeuwen/go-bindata/...
	go get github.com/elazarl/go-bindata-assetfs/...
	go get bitbucket.org/tebeka/snowball/...

prereqs-dist: prereqs
	go get golang.org/x/tools/cmd/cover/...
	go get golang.org/x/tools/cmd/vet/...
	go get github.com/aktau/github-release/...
	pip install mkdocs
