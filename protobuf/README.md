## Instructions for generating new go and gRPC stubs using search.proto

1. Download latest of protoc-gen-go and protoc-gen-go-grpc
```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

2. To generate `search.pb.go` using search.proto:
```
protoc --go_out=. --go_opt=Mprotobuf/search.proto=protobuf/ protobuf/search.proto
```

3. To generate `search_grpc.pb.go` using search.proto:
```
protoc --go-grpc_out=. --go-grpc_opt=Mprotobuf/search.proto=protobuf/ protobuf/search.proto
```
