BINARY_NAME := nats-s3
BUILD_DIR := bin
VERSION ?= $(shell git describe --always --tags 2>/dev/null || echo dev)

all: build test

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

build: $(BUILD_DIR)
	go build -ldflags="-X main.Version=$(VERSION)" -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/nats-s3

fmt:
	@files=$$(gofmt -s -l .); if [ -n "$$files" ]; then \
		echo "The following files are not gofmt-ed:"; echo "$$files"; exit 1; \
	else echo "gofmt check passed"; fi

format fmt-fix:
	gofmt -s -w .

vet:
	go vet ./...

test: build fmt vet
	go test -race -count=1 ./...

run: build
	$(BUILD_DIR)/$(BINARY_NAME) --listen 0.0.0.0:5222 --natsServers nats://127.0.0.1:4222

clean:
	rm -rf $(BUILD_DIR)
