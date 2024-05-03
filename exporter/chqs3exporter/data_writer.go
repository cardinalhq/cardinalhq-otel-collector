// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"
	"io"
)

type dataWriter interface {
	writeBuffer(ctx context.Context, buf io.Reader, config *Config, metadata string, format string, kv map[string]string) error
}
