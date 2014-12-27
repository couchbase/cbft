default: build

build:
	go build ./...
	go build -o ./cbft ./cmd/cbft

build-leveldb:
	go build ./...
	go build -tags leveldb -o ./cbft ./cmd/cbft

build-forestdb:
	go build ./...
	go build -tags forestdb -o ./cbft ./cmd/cbft

test:
	go test -v ./...

coverage:
	go test -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out
