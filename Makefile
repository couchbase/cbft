default: build

build:
	go build ./...
	go build -o ./cbft ./cmd/cbft

build-leveldb:
	go build -tags 'debug leveldb' ./...
	go build -tags 'debug leveldb' -o ./cbft ./cmd/cbft

build-forestdb:
	go build -tags 'debug forestdb' ./...
	go build -tags 'debug forestdb' -o ./cbft ./cmd/cbft

build-full:
	go build -tags 'debug leveldb forestdb' ./...
	go build -tags 'debug leveldb forestdb' -o ./cbft ./cmd/cbft

test:
	go test -v ./...

test-full:
	go test -v -tags 'debug leveldb forestdb' ./...

coverage:
	go test -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out
