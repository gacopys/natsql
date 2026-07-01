.PHONY: all build vet test format-check coverage examples

all: format-check build vet test coverage examples

format-check:
	@if [ "$(shell gofmt -l . | wc -l)" -ne 0 ]; then \
		echo "Unformatted Go source files:"; \
		gofmt -l .; \
		exit 1; \
	fi

build:
	go build ./...

vet:
	go vet ./...

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
