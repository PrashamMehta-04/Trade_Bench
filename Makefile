GO_BIN := $(shell pwd)/tools/go/bin/go
PATH := $(shell pwd)/tools/go/bin:$(PATH)

.PHONY: all build clean

all: build

build:
	$(GO_BIN) build -o bin/orchestrator cmd/orchestrator/main.go
	$(GO_BIN) build -o bin/load-generator cmd/load-generator/main.go
	$(GO_BIN) build -o bin/ingester cmd/ingester/main.go

clean:
	rm -rf bin/
