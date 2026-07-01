.PHONY: all build lint lint-fix vuln test coverage examples gocyclo dupl generate

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

ARTDUPL := $(shell go env GOPATH)/bin/art-dupl

.PHONY: dupl-install
dupl-install:
	@which $(ARTDUPL) >/dev/null 2>&1 || go install github.com/LarsArtmann/art-dupl/cmd/art-dupl@v0.2.0

dupl: dupl-install
	@echo "━━━ Code duplication report (production code, 50-token threshold) ━━━"
	@$(ARTDUPL) -t 50 --exclude-pattern '*_test.go' internal/ cmd/ *.go 2>&1

# ---------------------------------------------------------------------------
# OpenAPI code generation
# ---------------------------------------------------------------------------

OAPI_CODEGEN := $(shell go env GOPATH)/bin/oapi-codegen
OAPI_CONFIG := oapi-codegen.yaml
OAPI_SPEC := openapi.yaml
OAPI_OUTPUT := internal/transport/oapi/gen.go

.PHONY: oapi-install
oapi-install:
	@which $(OAPI_CODEGEN) >/dev/null 2>&1 || go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.7.1

# generate regenerates the chi server interface and models from openapi.yaml
# using the pinned oapi-codegen config (see OAPI_CONFIG). Run after editing
# openapi.yaml so the Go API stays in sync with the spec.
generate: oapi-install
	@echo "━━━ Generating OpenAPI server interface ━━━"
	@$(OAPI_CODEGEN) --config $(OAPI_CONFIG) $(OAPI_SPEC)
	@PATH="$(shell go env GOPATH)/bin:$(PATH)" gofumpt -w $(OAPI_OUTPUT) 2>/dev/null || true
	@echo "  Generated $(OAPI_OUTPUT)"
