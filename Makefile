CBFT_CHECKOUT = origin/master
CBFT_DOCKER   = cbft-builder:latest
CBFT_OUT      = ./cbft
CBFT_TAGS     =

pwd     = $(shell pwd)
version = $(shell git describe --long)
goflags = -ldflags '-X main.VERSION $(version)' \
          -tags "debug kagome $(CBFT_TAGS)"

# -------------------------------------------------------------------
# Targets commonly used for day-to-day development...

default: build

clean:
	rm -f ./cbft ./cbft_docs

build: gen-bindata
	go build $(goflags) -o $(CBFT_OUT) ./cmd/cbft

build-static:
	$(MAKE) build CBFT_TAGS="libstemmer"

build-forestdb:
	$(MAKE) build CBFT_TAGS="libstemmer icu forestdb"

build-leveldb:
	$(MAKE) build CBFT_TAGS="libstemmer icu leveldb"

build-full:
	$(MAKE) build CBFT_TAGS="full"

gen-bindata:
	go-bindata-assetfs -pkg=cbft ./static/...
	go fmt bindata_assetfs.go

gen-docs: cmd/cbft_docs/main.go
	go build -o ./cbft_docs ./cmd/cbft_docs
	./cbft_docs > docs/api-ref.md
	./dist/gen-command-docs > docs/admin-guide/command.md

test:
	go test -v -tags "debug kagome $(CBFT_TAGS)" .
	go test -v -tags "debug kagome $(CBFT_TAGS)" ./cmd/cbft

test-full:
	$(MAKE) test CBFT_TAGS="full"

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
	cp ./static/dist/version.txt ./dist/out
	./dist/go-manifest > ./static/dist/manifest.txt
	cp ./static/dist/manifest.txt ./dist/out
	cp ./CHANGES.md ./dist/out

dist-build:
	$(MAKE) build        GOOS=darwin  GOARCH=amd64       CBFT_OUT=./dist/out/cbft.darwin.amd64
	$(MAKE) build        GOOS=linux   GOARCH=386         CBFT_OUT=./dist/out/cbft.linux.386
	$(MAKE) build        GOOS=linux   GOARCH=arm         CBFT_OUT=./dist/out/cbft.linux.arm
	$(MAKE) build        GOOS=linux   GOARCH=arm GOARM=5 CBFT_OUT=./dist/out/cbft.linux.arm5
	$(MAKE) build        GOOS=linux   GOARCH=amd64       CBFT_OUT=./dist/out/cbft.linux.amd64
	$(MAKE) build-static GOOS=linux   GOARCH=amd64       CBFT_OUT=./dist/out/cbft-full.linux.amd64
	$(MAKE) build        GOOS=freebsd GOARCH=amd64       CBFT_OUT=./dist/out/cbft.freebsd.amd64
	$(MAKE) build        GOOS=windows GOARCH=386         CBFT_OUT=./dist/out/cbft.windows.386.exe
	$(MAKE) build        GOOS=windows GOARCH=amd64       CBFT_OUT=./dist/out/cbft.windows.amd64.exe

dist-clean: clean
	rm -rf ./dist/out/*
	rm -rf ./static/dist/*
	git checkout bindata_assetfs.go

# The release target prerequisites...
#
# - A cbft-builder docker image.
#
# See: ./dist/Dockerfile
#
# - Access tokens to be able to publish releases on couchbaselabs/cbft...
#
#   export GITHUB_TOKEN=/* a github access token */
#   export GITHUB_USER=couchbaselabs
#
# See: https://help.github.com/articles/creating-an-access-token-for-command-line-use
#
# To release a new version...
#
#   git grep v0.0.1 # Look for old version strings.
#   git grep v0.0   # Look for old version strings.
#   # Edit/update files, especially cmd/cbft/main.go and mkdocs.yml...
#   # Then, ensure that bindata_assetfs.go is up-to-date, by running...
#   make build
#   # Then, run tests, etc...
#   make test
#   # Then, run a diff against the previous version...
#   git log v0.0.1...
#   # Then, update the CHANGES.md file based on diff.
#   git commit -m "v0.0.2"
#   git push
#   git tag -a "v0.0.2" -m "v0.0.2"
#   git push --tags
#   make release
#
# Remember, we use semver versioning rules.
#
# Of note, the version.go/VERSION is only updated on data/config format changes.
#
release: release-build \
	release-github-register release-github-upload release-github-docs

release-build:
	mkdir -p ./tmp/dist-out
	mkdir -p ./tmp/dist-site
	rm -rf ./tmp/dist-out/*
	rm -rf ./tmp/dist-site/*
	docker run --rm \
		-v $(pwd)/tmp/dist-out:/tmp/dist-out \
		-v $(pwd)/tmp/dist-site:/tmp/dist-site \
		$(CBFT_DOCKER) \
		make -C /go/src/github.com/couchbaselabs/cbft \
			CBFT_CHECKOUT=$(CBFT_CHECKOUT) \
			release-build-helper dist-clean
	(cd ./tmp/dist-out; for f in *.exe; do \
		zip $$f.zip $$f; \
	done)
	(cd ./tmp/dist-out; for f in *.amd64; do \
		tar -zcvf $$f.tar.gz $$f; \
	done)

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

release-github-register:
	$(GOPATH)/bin/github-release --verbose release \
		--repo cbft \
		--tag $(strip $(shell git describe --abbrev=0 --tags \
				$(strip $(shell cat ./tmp/dist-out/version.txt)))) \
		--pre-release

release-github-upload:
	(cd ./tmp/dist-out; for f in *.gz *.zip *.md; do \
		echo $$f | \
			sed -e s/\\./-$(strip $(shell cat ./tmp/dist-out/version.txt))\\./1 | \
			xargs $(GOPATH)/bin/github-release upload \
				--file $$f \
				--repo cbft \
				--tag $(strip $(shell git describe --abbrev=0 --tags \
						$(strip $(shell cat ./tmp/dist-out/version.txt)))) \
				--name; \
	done)

release-github-docs:
	rm -rf ./site/*
	cp -R ./tmp/dist-site/* ./site
	mkdocs gh-deploy

# -------------------------------------------------------------------
# The prereqs are for one time setup of required build/dist tools...

prereqs:
	go get golang.org/x/tools/cmd/vet/...
	go get golang.org/x/tools/cmd/cover/...
	go get github.com/jteeuwen/go-bindata/...
	go get github.com/elazarl/go-bindata-assetfs/...
	go get github.com/ikawaha/kagome/...
	go get bitbucket.org/tebeka/snowball/...

prereqs-dist: prereqs
	go get github.com/aktau/github-release/...
	pip install mkdocs
