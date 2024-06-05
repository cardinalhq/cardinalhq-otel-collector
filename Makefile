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
PLATFORM=linux/amd64,linux/arm64
BUILDX=docker buildx build --pull --platform ${PLATFORM}
IMAGE_PREFIX=us-central1-docker.pkg.dev/profound-ship-384422/cardinalhq/

#
# Build targets.  Adding to these will cause magic to occur.
#

# These are targets for "make local"
BINARIES = cardinalhq-otel-collector

# These are the targets for Docker images, used both for the multi-arch and
# single (local) Docker builds.
# Dockerfiles should have a target that ends in -image, e.g. agent-image.
IMAGE_TARGETS = cardinalhq-otel-collector

LINT_TEST_SOURCE_PATHS = `ls -1d {receiver,processor,exporter}/*` internal

#
# Below here lies magic...
#

rl_deps = internal/tokenizer/fingerprinter.go

all_deps := $(shell find . -name '*.yaml') Dockerfile Makefile

now := $(shell date -u +%Y%m%dT%H%M%S)

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

#
# Run pre-commit checks
#
check: test
	license-eye header check
	for i in $(LINT_TEST_SOURCE_PATHS); do \
		(echo ============ linting $$i ... ; cd $$i && golangci-lint run) \
	done


# requires otel builder to be installed.
# go install go.opentelemetry.io/collector/cmd/builder@latest
bin/cardinalhq-otel-collector: cardinalhq-otel-collector.yaml
	CGO_ENABLED=0 go run go.opentelemetry.io/collector/cmd/builder@latest --config cardinalhq-otel-collector.yaml 
#
# make a buildtime directory to hold the build timestamp files
buildtime:
	[ ! -d buildtime ] && mkdir buildtime

#
# set git info details
#
set-git-info:
	@$(eval GIT_BRANCH=$(shell git describe --tags))


#
# Multi-architecture image builds
#
.PHONY: images
images: buildtime clean-image-names set-git-info $(addsuffix .tstamp, $(addprefix buildtime/,$(IMAGE_TARGETS)))

buildtime/%.tstamp:: ${all_deps} Dockerfile
	${BUILDX} \
		--tag ${IMAGE_PREFIX}$(patsubst %.tstamp,%,$(@F)):latest \
		--tag ${IMAGE_PREFIX}$(patsubst %.tstamp,%,$(@F)):${GIT_BRANCH} \
		--target $(patsubst %.tstamp,%,$(@F))-image \
		--build-arg IMAGE_PREFIX=${IMAGE_PREFIX} \
		--build-arg BUILD_TYPE=release \
		-f Dockerfile \
		--push .
	echo >> buildtime/image-names.txt ${IMAGE_PREFIX}$(patsubst %.tstamp,%,$(@F)):latest
	echo >> buildtime/image-names.txt ${IMAGE_PREFIX}$(patsubst %.tstamp,%,$(@F)):${GIT_BRANCH}
	@touch $@

pre-build:
	${BUILDX} \
		--tag ${IMAGE_PREFIX}builder-${IMAGE_TARGETS}:latest \
		--build-arg BUILD_TYPE=release \
		-f Dockerfile.pre-build \
		--push .

.PHONY: image-names
image-names:
	@echo ::set-output name=imageNames::$(shell echo `cat buildtime/image-names.txt` | sed 's/\ /,\ /g')

#
# Test targets
#

.PHONY: test
test: generate
	for i in $(LINT_TEST_SOURCE_PATHS); do \
		(echo ============ testing $$i ... ; cd $$i && go test ./...) \
	done

#
# Clean the world.
#

.PHONY: clean
clean: clean-image-names
	rm -f buildtime/*.tstamp
	rm -f bin/*

.PHONY: really-clean
really-clean: clean

.PHONY: clean-image-names
clean-image-names:
	rm -f buildtime/image-names.txt
