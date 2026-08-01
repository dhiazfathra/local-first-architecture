.PHONY: test lint proto demo demo-down

test:
	CGO_ENABLED=0 go test ./... -cover

lint:
	golangci-lint run

proto:
	protoc --go_out=. --go_opt=module=github.com/dhiazfathra/local-first-architecture/localfirst-go \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/dhiazfathra/local-first-architecture/localfirst-go \
	       sync/sync.proto transport/grpc/node.proto

demo:
	docker compose up -d --build
	go run ./demo/cmd/demo; status=$$?; docker compose down -v; exit $$status

demo-down:
	docker compose down -v
