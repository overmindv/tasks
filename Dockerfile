FROM golang:1.26-alpine AS build

ARG GOPROXY
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}
# Лимит параллельных компиляций пакетов: на машинах с малым объёмом RAM полный
# параллелизм go build (по числу ядер) вызывает Docker OOM (signal: killed).
# Компилируемые пакеты кэшируются в слое между пересборками сервисов (общий id).
ARG GO_BUILD_PARALLEL=2
ENV GOFLAGS=-p=${GO_BUILD_PARALLEL}

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,id=overmindv-gomod,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,id=overmindv-gomod,target=/go/pkg/mod \
    --mount=type=cache,id=overmindv-gobuild,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tasks ./cmd/tasks

FROM alpine:3.23

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S tasks \
    && adduser -S tasks -G tasks
WORKDIR /app
COPY --from=build /out/tasks /usr/local/bin/tasks
COPY migrations /app/migrations
USER tasks
EXPOSE 8080
ENTRYPOINT ["tasks"]
