BINARY  := dcs
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build install test vet fmt clean serve-ui

build: ## Build ./dcs for the host platform
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/dcs

install: ## Install dcs into $(GOPATH)/bin
	go install -ldflags "$(LDFLAGS)" ./cmd/dcs

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -f $(BINARY)

serve-ui: ## Serve docs/ locally at http://localhost:8080
	go run ./internal/uiserve -dir docs -addr :8080
