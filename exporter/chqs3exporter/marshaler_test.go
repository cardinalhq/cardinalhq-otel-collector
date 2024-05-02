// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

func TestMarshaler(t *testing.T) {
	{
		m, err := newMarshaler(zap.NewNop())
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, m.format(), "parquet")
	}
}

type hostWithExtensions struct {
	encoding encodingExtension
}

func (h hostWithExtensions) Start(context.Context, component.Host) error {
	panic("unsupported")
}

func (h hostWithExtensions) Shutdown(context.Context) error {
	panic("unsupported")
}

func (h hostWithExtensions) GetFactory(component.Kind, component.Type) component.Factory {
	panic("unsupported")
}

func (h hostWithExtensions) GetExtensions() map[component.ID]component.Component {
	return map[component.ID]component.Component{
		component.MustNewID("foo"): h.encoding,
	}
}

func (h hostWithExtensions) GetExporters() map[component.DataType]map[component.ID]component.Component {
	panic("unsupported")
}

type encodingExtension struct {
}

func (e encodingExtension) Start(_ context.Context, _ component.Host) error {
	panic("unsupported")
}

func (e encodingExtension) Shutdown(_ context.Context) error {
	panic("unsupported")
}
