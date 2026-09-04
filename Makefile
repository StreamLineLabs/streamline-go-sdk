.PHONY: integration-test build test lint fmt clean help vet examples vuln

GOVULNCHECK_VERSION ?= v1.7.0
GO_SECURITY_TOOLCHAIN ?= go1.25.14
TESTCONTAINERS_GO_TOOLCHAIN ?= go1.26.6

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the SDK
	go build ./...
	cd testcontainers && GOTOOLCHAIN=$(TESTCONTAINERS_GO_TOOLCHAIN) go build ./...

test: ## Run unit tests (no server or Docker required)
	go test ./...
	cd testcontainers && GOTOOLCHAIN=$(TESTCONTAINERS_GO_TOOLCHAIN) go test ./...

lint: vet ## Run linting
	golangci-lint run ./...

vet: ## Run go vet (default build and the integration-tagged conformance suite)
	go vet ./...
	go vet -tags=integration ./...
	cd testcontainers && GOTOOLCHAIN=$(TESTCONTAINERS_GO_TOOLCHAIN) go vet ./...

examples: ## Compile runnable programs and documentation examples
	go build ./examples/...
	go test -run '^Example' ./...
	cd testcontainers && GOTOOLCHAIN=$(TESTCONTAINERS_GO_TOOLCHAIN) go test -run '^Example' ./...

vuln: ## Scan root and testcontainers modules for reachable vulnerabilities
	GOTOOLCHAIN=$(GO_SECURITY_TOOLCHAIN) go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...
	cd testcontainers && GOTOOLCHAIN=$(TESTCONTAINERS_GO_TOOLCHAIN) go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

fmt: ## Format code
	go fmt ./...

clean: ## Clean build cache
	go clean -cache

check: fmt vet test ## Run all checks

integration-test: ## Run the conformance suite against a live server (requires Docker)
	@set -eu; \
	cleanup() { docker compose -f docker-compose.test.yml down -v; }; \
	trap cleanup EXIT; \
	docker compose -f docker-compose.test.yml up -d; \
	echo "Waiting for Streamline server..."; \
	ready=false; \
	for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health > /dev/null 2>&1; then \
			echo "Server ready"; \
			ready=true; \
			break; \
		fi; \
		echo "Waiting for Streamline... ($$i/30)"; \
		sleep 2; \
	done; \
	if [ "$$ready" != "true" ]; then \
		echo "Streamline failed to become healthy"; \
		docker compose -f docker-compose.test.yml logs; \
		exit 1; \
	fi; \
	STREAMLINE_REQUIRE_INTEGRATION=true go test -v -count=1 -tags=integration ./... -timeout 120s
