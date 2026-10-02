.PHONY: build test test-unit test-int vet fmt lint up down

BINARY := sampling-server
export GOTOOLCHAIN := local

build:
	go build -o $(BINARY) ./cmd/server

# Pure-logic tests (no database required).
test-unit:
	go test ./internal/plan/... ./internal/dist/... ./internal/oc/... \
	        ./internal/design/... ./internal/inspection/...

# PostgreSQL integration tests; point PG_TEST_DSN at a PG16 instance.
# Example:
#   PG_TEST_DSN="host=localhost port=5432 user=sampling dbname=sampling sslmode=disable" make test-int
test-int:
	go test -race -count=1 ./internal/store/... ./internal/httpapi/...

test: test-unit test-int

vet:
	go vet ./...

fmt:
	gofmt -w internal/ cmd/

up:
	docker compose up --build -d

down:
	docker compose down -v
