FROM golang:1.26-alpine AS build

ARG GOPROXY
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tasks ./cmd/tasks

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
