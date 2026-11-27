JET_SCHEMA ?= public
JET_OUTPUT_DIR ?= ./internal/adapter/postgres/generated

LOCAL_BIN := $(CURDIR)/bin
GOLANGCI_LINT := $(LOCAL_BIN)/golangci-lint
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/tasks?sslmode=disable
COMPONENT_TEST_DSN ?=

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
# Требует запущенного PostgreSQL с созданной БД tasks (см. docs/development.md).
dev: migrate-up run

# run — запускает HTTP API; конфигурация берётся из .env.
run:
	go run ./cmd/tasks

# build — проверяет, что весь проект компилируется.
build:
	go build ./...

# test — обычные unit/use-case тесты (с детектором гонок).
test:
	go test -race ./...

# ctest — component-тесты: применяет миграции к ВЫДЕЛЕННОЙ тестовой БД
# и гоняет полный HTTP-сценарий. Не направляйте на общую/production database.
ctest:
	@test -n "$(COMPONENT_TEST_DSN)" || (echo "Set COMPONENT_TEST_DSN to a dedicated _test database"; exit 1)
	go run ./cmd/tasks migrate --dir migrations --dsn "$(COMPONENT_TEST_DSN)" up
	COMPONENT_TEST_DSN="$(COMPONENT_TEST_DSN)" go test -count=1 -tags=component ./tests/component/...

# lint — статический анализ (gofmt, goimports, staticcheck и др.).
lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run

# migrate-up / migrate-down — применяют или откатывают миграции к DATABASE_URL (via parker).
migrate-up:
	go run ./cmd/tasks migrate --dir migrations --dsn "$(DATABASE_URL)" up

migrate-down:
	go run ./cmd/tasks migrate --dir migrations --dsn "$(DATABASE_URL)" down

# jet-generate — перегенерирует типизированные Jet-модели из live-schema БД.
# Нужен только при изменении схемы; требует поднятого PostgreSQL.
jet-generate:
	@test -n "$$JET_DATABASE_DSN" || { echo "JET_DATABASE_DSN is required" >&2; exit 1; }
	@go tool jet -source=postgres -dsn="$$JET_DATABASE_DSN" -schema="$(JET_SCHEMA)" -path="$(JET_OUTPUT_DIR)" >/dev/null 2>&1 || { echo "Jet generation failed; check database access and schema" >&2; exit 1; }

# tidy — приводит go.mod/go.sum в соответствие с импортами.
tidy:
	go mod tidy

# Установка вспомогательных инструментов в bin/ (один раз, gitignored).
$(GOLANGCI_LINT):
	GOBIN="$(LOCAL_BIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
