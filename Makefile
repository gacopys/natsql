.PHONY: all build lint test coverage examples

all: lint build test coverage examples

build:
	go build ./...

lint:
	golangci-lint run ./...

test:
	go test -race -count=1 -coverprofile=coverage.out -v ./...

coverage:
	@echo ""
	@echo "━━━ Coverage per function ━━━"
	@go tool cover -func=coverage.out

examples:
	@for dir in examples/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Building $$dir..."; \
			(cd "$$dir" && go build .); \
		else \
			echo "Skipping $$dir (no go.mod)"; \
		fi \
	done
