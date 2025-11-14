ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# Speed up module download with BuildKit caches when available
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Copy source and build directly with go build
COPY . .
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/nats-s3 ./cmd/nats-s3

FROM alpine:3.22
# Create non-root user to run the service
RUN adduser -D -H -s /sbin/nologin app
USER app
WORKDIR /home/app

COPY --from=build /out/nats-s3 /usr/local/bin/nats-s3

EXPOSE 5222
ENTRYPOINT ["nats-s3"]
CMD ["--listen","0.0.0.0:5222","--natsServers","nats://127.0.0.1:4222"]
