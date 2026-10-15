.PHONY: all build test race cover run-mock run-server docker-build docker-up docker-down clean

BINARY_SERVER    := bin/server
BINARY_MOCK      := bin/mockserver
GO               := go
GOFLAGS          :=

all: build

## build: compile both binaries into ./bin/
build:
	@mkdir -p bin
	$(GO) build $(GOFLAGS) -o $(BINARY_SERVER)   ./cmd/server
	$(GO) build $(GOFLAGS) -o $(BINARY_MOCK)     ./cmd/mockserver
	@echo "✓  binaries written to ./bin/"

## test: run all tests (no race detector, fast)
test:
	$(GO) test ./... -v -timeout 60s

## race: run all tests with the data-race detector enabled
race:
	$(GO) test -race ./... -v -timeout 120s

## cover: generate HTML coverage report and open it
cover:
	$(GO) test -race -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	open coverage.html

## run-mock: start the mock downstream TCP server on :9090
run-mock: build
	./$(BINARY_MOCK) -addr :9090 -fail 0.05

## run-server: start the HTTP job queue server (requires mock running)
run-server: build
	LISTEN_ADDR=:8080 DOWNSTREAM_ADDR=:9090 POOL_SIZE=5 ./$(BINARY_SERVER)

## docker-build: build both Docker images
docker-build:
	docker build -t job-queue-service:latest .

## docker-up: start the full stack with Docker Compose
docker-up:
	docker compose up --build -d
	@echo "✓  services running — API at http://localhost:8080"

## docker-down: stop and remove containers
docker-down:
	docker compose down

## clean: remove build artefacts
clean:
	rm -rf bin/ coverage.out coverage.html

## help: print this help message
help:
	@grep -E '^## ' Makefile | sed 's/## //'
