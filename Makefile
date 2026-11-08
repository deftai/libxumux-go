# libxumux-go — see `make help`
COVER_MIN := 75
LIBXUMUX_DIR ?=
XUMUX_DIR ?=
FUZZTIME ?= 30s

.PHONY: help go/fmt go/vet test test/race test/coverage test/ws test/webrtc test/interop test/conformance test/fuzz check clean

help: ## Show targets
	@grep -E '^[a-zA-Z/_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n",$$1,$$2}'

go/fmt: ## Format
	gofmt -w .

go/vet: ## Vet
	go vet ./...

test: ## Run tests
	go test -count=1 ./...

test/race: ## Run tests with the race detector
	go test -race -count=1 ./...

test/coverage: ## Coverage report; fails below $(COVER_MIN)%
	go test -count=1 -coverprofile=coverage.out $$(go list ./... | grep -v /conformance/) | tee coverage.txt
	go tool cover -html=coverage.out -o coverage.html
	@awk -v min=$(COVER_MIN) '/coverage:/ { for (i=1;i<=NF;i++) if ($$i=="coverage:") { p=$$(i+1); sub("%","",p); if (p+0 < min) { print "below " min "%: " $$0; bad=1 } } } END { exit bad }' coverage.txt

test/ws: ## Vet and race-test the nested ws module (not reached by ./...)
	cd ws && go vet ./... && go test -race -count=1 ./...

test/interop: ## Interop tests against libxumux (LIBXUMUX_DIR=path/to/built/libxumux)
	@test -n "$(LIBXUMUX_DIR)" || (echo "set LIBXUMUX_DIR to a built libxumux checkout" && exit 1)
	LIBXUMUX_DIR=$(abspath $(LIBXUMUX_DIR)) go test -count=1 -run Interop -v .

test/conformance: ## Behavioural conformance suite (XUMUX_DIR=path/to/deftai/xumux checkout)
	@test -n "$(XUMUX_DIR)" || (echo "set XUMUX_DIR to a deftai/xumux checkout" && exit 1)
	go build -o conformance-adapter ./conformance/adapter
	python3 -I $(abspath $(XUMUX_DIR))/conformance/behaviour/run.py -- $(abspath conformance-adapter)

test/fuzz: ## Fuzz every target (FUZZTIME per target, default 30s)
	@list=$$(go test -list '^Fuzz' .) || { echo "$$list"; echo "cannot list fuzz targets"; exit 1; }; \
	targets=$$(echo "$$list" | grep '^Fuzz'); \
	[ -n "$$targets" ] || { echo "no fuzz targets"; exit 1; }; \
	for f in $$targets; do \
	  echo "== $$f"; \
	  go test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) . || exit 1; \
	done

test/webrtc: ## Vet and race-test the webrtc module (pion, in-process PeerConnections)
	cd webrtc && go vet ./... && go test -race -count=1 ./...

check: go/fmt go/vet test/race test/coverage test/ws test/webrtc ## Pre-commit gate: fmt, vet, race tests, coverage, nested modules

clean: ## Remove coverage output
	rm -f coverage.out coverage.txt coverage.html conformance-adapter
