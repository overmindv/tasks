LOCAL_BIN := $(CURDIR)/bin
GOOSE := $(LOCAL_BIN)/goose
JET := $(LOCAL_BIN)/jet
GOLANGCI_LINT := $(LOCAL_BIN)/golangci-lint
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/tasks_it?sslmode=disable
COMPONENT_TEST_DSN ?= $(DATABASE_URL)

# Автоматически подхватываем локальный .env (KEY=value), чтобы не выполнять
# "set -a && source .env && set +a" вручную перед каждым таргетом.
# Значения из .env доступны и как make-переменные, и в окружении процессов
# (export), поэтому go run и goose читают конфигурацию без дополнительных шагов.
ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: dev run build test ctest lint migrate-up migrate-down jet-generate tidy

# dev — главный локальный таргет: применяет миграции и запускает сервис.
# Требует запущенного PostgreSQL с созданной БД tasks_it (см. docs/development.md).
dev: migrate-up run

# run — запускает HTTP API; конфигурация берётся из .env.
run:
	go run ./cmd/tasks-it

# build — проверяет, что весь проект компилируется.
build:
	go build ./...

# test — обычные unit/use-case тесты (с детектором гонок).
test:
	go test -race ./...

# ctest — component-тесты: применяет миграции к ВЫДЕЛЕННОЙ тестовой БД
# и гоняет полный HTTP-сценарий. Не направляйте на общую/production database.
ctest: $(GOOSE)
	$(GOOSE) -dir migrations postgres "$(COMPONENT_TEST_DSN)" up
	COMPONENT_TEST_DSN="$(COMPONENT_TEST_DSN)" go test -tags=component ./tests/component/...

# lint — статический анализ (gofmt, goimports, staticcheck и др.).
lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run

# migrate-up / migrate-down — применяют или откатывают миграции к DATABASE_URL.
migrate-up: $(GOOSE)
	$(GOOSE) -dir migrations postgres "$(DATABASE_URL)" up

migrate-down: $(GOOSE)
	$(GOOSE) -dir migrations postgres "$(DATABASE_URL)" down

# jet-generate — перегенерирует типизированные Jet-модели из live-schema БД.
# Нужен только при изменении схемы; требует поднятого PostgreSQL.
jet-generate: $(JET)
	$(JET) -source=PostgreSQL -dsn="$(DATABASE_URL)" -schema=public -path=./internal/adapter/postgres/generated

# tidy — приводит go.mod/go.sum в соответствие с импортами.
tidy:
	go mod tidy

# Установка вспомогательных инструментов в bin/ (один раз, gitignored).
$(GOOSE):
	GOBIN="$(LOCAL_BIN)" go install -tags="no_clickhouse no_mssql no_mysql no_sqlite3 no_libsql no_ydb no_vertica" github.com/pressly/goose/v3/cmd/goose@v3.24.3

$(JET):
	GOBIN="$(LOCAL_BIN)" go install github.com/go-jet/jet/v2/cmd/jet@v2.14.0

$(GOLANGCI_LINT):
	GOBIN="$(LOCAL_BIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
