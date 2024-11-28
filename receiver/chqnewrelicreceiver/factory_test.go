// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicreceiver // Adjust the package import path as necessary

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestCreateReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	cfg.(*Config).Endpoint = "http://localhost:0"

	tReceiver, err := factory.CreateTraces(context.Background(), receivertest.NewNopSettings(), cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, tReceiver, "trace receiver creation failed")

	lReceiver, err := factory.CreateLogs(context.Background(), receivertest.NewNopSettings(), cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, lReceiver, "log receiver creation failed")

	mReceiver, err := factory.CreateMetrics(context.Background(), receivertest.NewNopSettings(), cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, mReceiver, "metric receiver creation failed")
}
