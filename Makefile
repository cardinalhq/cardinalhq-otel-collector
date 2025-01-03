# Copyright 2024 CardinalHQ, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

TARGETS=bin/cardinalhq-otel-collector
OTEL_VERSION=v0.116.0

#
# Build targets.  Adding to these will cause magic to occur.
#

# These are targets for "make local"
BINARIES = cardinalhq-otel-collector

MODULE_SOURCE_PATHS = `ls -1d {receiver,processor,exporter,extension}/*` internal
SUMFILES = $(shell ls -1 {receiver,processor,exporter,extension}/*/go.sum internal/go.sum)
ALLGOFILES = $(shell find ${MODULE_SOURCE_PATHS} -name '*.go')

#
# Below here lies magic...
#

all_deps := $(shell find . -name '*.yaml') Dockerfile Makefile distribution/main.go ${SUMFILES} $(ALLGOFILES)

#
# Default target.
#

.PHONY: all
all: ${TARGETS}

#
# Generate all the things.
#
.PHONY: generate
generate:
	for i in $(MODULE_SOURCE_PATHS); do \
		(echo ============ generating $$i ... ; cd $$i && go generate ./...) || exit 1; \
	done

.PHONY: fmt
fmt:
	for i in $(MODULE_SOURCE_PATHS); do \
		(echo ============ formatting $$i ... ; cd $$i && gci write . --skip-generated -s standard -s default -s 'Prefix(github.com/cardinalhq/cardinalhq-otel-collector)') || exit 1; \
	done

#
# Run pre-commit checks
#
check: test license-check lint

license-check:
	license-eye header check

lint:
	for i in $(MODULE_SOURCE_PATHS); do \
		(echo ============ linting $$i ... ; cd $$i && golangci-lint run) || exit 1; \
	done

update-deps:
	for i in $(MODULE_SOURCE_PATHS); do \
		(echo ============ updating $$i ... ; cd $$i && go get -u -t ./... && go mod tidy) || exit 1; \
	done

tidy:
	for i in $(MODULE_SOURCE_PATHS); do \
		(echo ============ go tidy in $$i ... ; cd $$i && go mod tidy) || exit 1; \
	done

.PHONY: buildfiles
buildfiles: ${SUMFILES} distribution/main.go
	rm -rf dist/*

distribution/main.go: ${SUMFILES} cardinalhq-otel-collector.yaml
	go run go.opentelemetry.io/collector/cmd/builder@${OTEL_VERSION} --config cardinalhq-otel-collector.yaml --skip-compilation
	sed -i.bak 's|$(shell pwd)|..|g' distribution/go.mod
	rm -f distribution/go.mod.bak

# requires otel builder to be installed.
# go install go.opentelemetry.io/collector/cmd/builder@latest
bin/cardinalhq-otel-collector: cardinalhq-otel-collector.yaml distribution/main.go ${all_deps}
	(cd distribution ; CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o ../$@ .)

#
# Multi-architecture image builds
# requires goreleaser to be installed.
#
.PHONY: images
images: buildfiles
	GITHUB_TOKEN=dummytoken goreleaser

#
# Test targets
#

.PHONY: test
test:
	for i in $(MODULE_SOURCE_PATHS); do \
		(echo ============ testing $$i ... && cd $$i && go test ./...) || exit 1; \
	done

#
# Clean the world.
#

.PHONY: clean
clean:
	rm -f bin/*

.PHONY: really-clean
really-clean: clean
	rm -rf distribution
