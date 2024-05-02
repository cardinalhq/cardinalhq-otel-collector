// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import "context"

type dataWriter interface {
	writeBuffer(ctx context.Context, buf []byte, config *Config, metadata string, format string) error
}
