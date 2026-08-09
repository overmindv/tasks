FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tasks-it ./cmd/tasks-it
RUN GOBIN=/out go install -tags="no_clickhouse no_mssql no_mysql no_sqlite3 no_libsql no_ydb no_vertica" github.com/pressly/goose/v3/cmd/goose@v3.24.3

FROM alpine:3.23

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S tasksit \
    && adduser -S tasksit -G tasksit
WORKDIR /app
COPY --from=build /out/tasks-it /usr/local/bin/tasks-it
COPY --from=build /out/goose /usr/local/bin/goose
COPY migrations /app/migrations
USER tasksit
EXPOSE 8080
ENTRYPOINT ["tasks-it"]
