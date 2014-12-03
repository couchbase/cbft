default: build

build:
	go build -o ./cbft ./cmd/cbft

test:
	go test -v ./...

coverage:
	go test -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out
