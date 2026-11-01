OCB_VERSION ?= v0.164.0
OCB ?= $(shell go env GOPATH)/bin/builder
BUILD_DIR := _build
BINARY := $(BUILD_DIR)/fusescope-collector
COMPONENTS := $(patsubst %/go.mod,%,$(wildcard components/*/*/go.mod))
LICENSES_ALLOWED := Apache-2.0,MIT,MIT-0,BSD-2-Clause,BSD-3-Clause,ISC,MPL-2.0
# Verified by hand: go-licenses cannot classify these licenses (ADR 0005, NOTICE).
LICENSES_UNCLASSIFIED := github.com/segmentio/asm

# Placeholder values so `validate` can check the example configs offline.
VALIDATE_ENV := INGEST_TOKENS_FILE=/dev/null ENGINE_TOKEN_FILE=/dev/null GATEWAY_TOKEN_FILE=/dev/null \
	TLS_CERT_FILE=/dev/null TLS_KEY_FILE=/dev/null ENGINE_ENDPOINT=engine:4317 \
	ENGINE_CA_FILE=/dev/null ENGINE_CLIENT_CERT_FILE=/dev/null ENGINE_CLIENT_KEY_FILE=/dev/null \
	GATEWAY_ENDPOINT=gateway:4317 GATEWAY_CA_FILE=/dev/null AGENT_CERT_FILE=/dev/null \
	AGENT_KEY_FILE=/dev/null K8S_NODE_NAME=node HOST_ROOT_PATH=/ KUBELET_AUTH_TYPE=none

.PHONY: build validate licenses vuln clean install-ocb components components-gen

install-ocb:
	go install go.opentelemetry.io/collector/cmd/builder@$(OCB_VERSION)

build: install-ocb
	$(OCB) --config builder-config.yaml

validate: $(BINARY)
	@for f in config/*.yaml; do echo ">> $$f"; env $(VALIDATE_ENV) $(BINARY) validate --config=$$f || exit 1; done

$(BINARY):
	$(MAKE) build

licenses: $(BINARY)
	cd $(BUILD_DIR) && go run github.com/google/go-licenses@v1.6.0 check ./... --allowed_licenses=$(LICENSES_ALLOWED) --ignore=github.com/fuseone-io/fusescope-collector-server $(addprefix --ignore=,$(LICENSES_UNCLASSIFIED))

vuln: $(BINARY)
	scripts/govulncheck.sh $(BINARY)

components:
	@for d in $(COMPONENTS); do echo ">> $$d"; \
	  (cd $$d && test -z "$$(gofmt -l .)" && go mod tidy -diff && go vet ./... && go test -race ./...) || { gofmt -l $$d; exit 1; }; \
	done

# mdatagen's module has replace directives, so `go run …@version` refuses it.
MDATAGEN := $(CURDIR)/$(BUILD_DIR)/tools/mdatagen

$(MDATAGEN):
	rm -rf $(BUILD_DIR)/tools/collector
	git clone -q --depth 1 -b $(OCB_VERSION) https://github.com/open-telemetry/opentelemetry-collector $(BUILD_DIR)/tools/collector
	cd $(BUILD_DIR)/tools/collector/cmd/mdatagen && go build -o $(MDATAGEN) .

components-gen: $(MDATAGEN)
	@for d in $(COMPONENTS); do (cd $$d && $(MDATAGEN) metadata.yaml) || exit 1; done

clean:
	rm -rf $(BUILD_DIR)
