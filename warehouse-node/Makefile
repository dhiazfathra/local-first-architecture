.PHONY: test lint proto
test:
	go test ./... -cover

lint:
	golangci-lint run

proto:
	protoc --go_out=. --go_opt=module=github.com/dhiazfathra/local-first-architecture/warehouse-node \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/dhiazfathra/local-first-architecture/warehouse-node \
	       proto/sync.proto proto/node_api.proto
