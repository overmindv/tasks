LOCAL_BIN := $(CURDIR)/bin
GOOSE := $(LOCAL_BIN)/goose
JET := $(LOCAL_BIN)/jet
GOLANGCI_LINT := $(LOCAL_BIN)/golangci-lint
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/tasks_it?sslmode=disable
COMPONENT_TEST_DSN ?= $(DATABASE_URL)

.PHONY: run build test ctest lint migrate-up migrate-down jet-generate tidy

run:
	go run ./cmd/tasks-it

build:
	go build ./...

test:
	go test -race ./...

ctest: $(GOOSE)
	$(GOOSE) -dir migrations postgres "$(COMPONENT_TEST_DSN)" up
	COMPONENT_TEST_DSN="$(COMPONENT_TEST_DSN)" go test -tags=component ./tests/component/...

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run

migrate-up: $(GOOSE)
	$(GOOSE) -dir migrations postgres "$(DATABASE_URL)" up

migrate-down: $(GOOSE)
	$(GOOSE) -dir migrations postgres "$(DATABASE_URL)" down

jet-generate: $(JET)
	$(JET) -source=PostgreSQL -dsn="$(DATABASE_URL)" -schema=public -path=./internal/adapter/postgres/generated

tidy:
	go mod tidy

$(GOOSE):
	GOBIN="$(LOCAL_BIN)" go install -tags="no_clickhouse no_mssql no_mysql no_sqlite3 no_libsql no_ydb no_vertica" github.com/pressly/goose/v3/cmd/goose@v3.24.3

$(JET):
	GOBIN="$(LOCAL_BIN)" go install github.com/go-jet/jet/v2/cmd/jet@v2.14.0

$(GOLANGCI_LINT):
	GOBIN="$(LOCAL_BIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
