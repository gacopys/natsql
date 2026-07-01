.PHONY: all build lint lint-fix vuln test coverage examples gocyclo

all: lint build test coverage examples

build:
	go build ./...

lint:
	@PATH="$(shell go env GOPATH)/bin:$(PATH)" golangci-lint run ./...

lint-fix:
	@PATH="$(shell go env GOPATH)/bin:$(PATH)" golangci-lint run --fix ./...

vuln:
	@PATH="$(shell go env GOPATH)/bin:$(PATH)" govulncheck ./...

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

GOCYCLO := $(shell go env GOPATH)/bin/gocyclo

.PHONY: gocyclo-install
gocyclo-install:
	@which $(GOCYCLO) >/dev/null 2>&1 || go install github.com/fzipp/gocyclo/cmd/gocyclo@latest

gocyclo: gocyclo-install
	@echo "━━━ Cyclomatic complexity report (threshold > 15) ━━━"
	@$(GOCYCLO) -over 15 . 2>/dev/null; \
	status=$$?; \
	echo ""; \
	echo "━━━ Score summary ━━━"; \
	total=$$($(GOCYCLO) . 2>/dev/null | wc -l); \
	over=$$($(GOCYCLO) -over 15 . 2>/dev/null | wc -l); \
	if [ "$$total" -gt 0 ]; then \
		good=$$((total - over)); \
		pct=$$((good * 100 / total)); \
		echo "  Total functions : $$total"; \
		echo "  Over threshold  : $$over"; \
		echo "  Score           : $$pct%"; \
	else \
		echo "  No functions analyzed"; \
	fi; \
	exit $$status
