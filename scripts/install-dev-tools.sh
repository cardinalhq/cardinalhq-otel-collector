#!/bin/bash
# Copyright 2024-2025 CardinalHQ, Inc
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

set -euo pipefail

# Dev tool versions - update these to change versions across the project
LICENSE_EYE_VERSION="latest"
GORELEASER_VERSION="latest"

# Project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$PROJECT_ROOT/bin"

echo "Installing development tools to $BIN_DIR..."

# Create bin directory if it doesn't exist
mkdir -p "$BIN_DIR"

# Install tools with pinned versions to project-local bin directory
echo "Installing license-eye $LICENSE_EYE_VERSION..."
if ! GOBIN="$BIN_DIR" go install "github.com/apache/skywalking-eyes/cmd/license-eye@$LICENSE_EYE_VERSION"; then
  echo "Failed to install license-eye" >&2
  exit 1
fi

echo "Installing goreleaser $GORELEASER_VERSION..."
if ! GOBIN="$BIN_DIR" go install "github.com/goreleaser/goreleaser/v2@$GORELEASER_VERSION"; then
  echo "Failed to install goreleaser" >&2
  exit 1
fi

echo ""
echo "Installation complete. Installed tools:"
ls -la "$BIN_DIR" | grep -E "license-eye|goreleaser" || echo "No tools found"
