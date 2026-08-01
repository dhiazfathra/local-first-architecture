PG_DSN ?= postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable

.PHONY: pg-up pg-down
pg-up:
	docker compose up -d postgres
	@until docker compose exec -T postgres pg_isready -U localfirst >/dev/null 2>&1; do sleep 0.3; done

pg-down:
	docker compose rm -sf postgres

.PHONY: test lint proto demo demo-down

test: pg-up
	LOCALFIRST_PG_DSN="$(PG_DSN)" CGO_ENABLED=0 go test ./... -cover

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
