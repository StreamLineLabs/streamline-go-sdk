.PHONY: integration-test build test lint fmt clean help vet

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

integration-test: ## Run integration tests (requires Docker)
	docker compose -f docker-compose.test.yml up -d
	@echo "Waiting for Streamline server..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health/live > /dev/null 2>&1; then \
			echo "Server ready"; \
			break; \
		fi; \
		sleep 2; \
	done
	go test -v -tags=integration ./... -timeout 120s || true
	docker compose -f docker-compose.test.yml down -v
