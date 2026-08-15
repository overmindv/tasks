FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tasks ./cmd/tasks
RUN GOBIN=/out go install -tags="no_clickhouse no_mssql no_mysql no_sqlite3 no_libsql no_ydb no_vertica" github.com/pressly/goose/v3/cmd/goose@v3.24.3

FROM alpine:3.23

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S tasks \
    && adduser -S tasks -G tasks
WORKDIR /app
COPY --from=build /out/tasks /usr/local/bin/tasks
COPY --from=build /out/goose /usr/local/bin/goose
COPY migrations /app/migrations
USER tasks
EXPOSE 8080
ENTRYPOINT ["tasks"]
