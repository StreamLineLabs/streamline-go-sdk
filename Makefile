.PHONY: build test lint fmt clean help vet

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the SDK
	go build ./...

test: ## Run tests
	go test ./...

lint: vet ## Run linting
	golangci-lint run ./...

vet: ## Run go vet
	go vet ./...

fmt: ## Format code
	go fmt ./...

clean: ## Clean build cache
	go clean -cache

check: fmt vet test ## Run all checks
