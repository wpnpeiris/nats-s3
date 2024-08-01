BINARY_NAME := nats-s3
BUILD_DIR := bin
GOLINT := golangci-lint
VERSION ?= $(shell git describe --always --tags)

all: build lint

build: 
	go build -ldflags="-X main.Version=$(VERSION)" -o $(BUILD_DIR)/$(BINARY_NAME)

lint:
	$(GOLINT) run
