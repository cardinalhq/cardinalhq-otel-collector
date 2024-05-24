// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datadogreceiver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestDatadogReceiver_Lifecycle(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	cfg.(*Config).Endpoint = "localhost:0"
	ddr, err := factory.CreateTracesReceiver(context.Background(), receivertest.NewNopCreateSettings(), cfg, consumertest.NewNop())
	assert.NoError(t, err, "Receiver should be created")

	err = ddr.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err, "Server should start")

	err = ddr.Shutdown(context.Background())
	assert.NoError(t, err, "Server should stop")
}

func TestDatadogServer(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "localhost:0" // Using a randomly assigned address
	dd, err := newDataDogReceiver(
		cfg,
		receivertest.NewNopCreateSettings(),
	)
	dd.(*datadogReceiver).nextTraceConsumer = consumertest.NewNop()
	require.NoError(t, err, "Must not error when creating receiver")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, dd.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, dd.Shutdown(ctx), "Must not error shutting down")
	})

	for _, tc := range []struct {
		name string
		op   io.Reader

		expectCode    int
		expectContent string
	}{
		{
			name:          "invalid data",
			op:            strings.NewReader("{"),
			expectCode:    http.StatusBadRequest,
			expectContent: `{"errors":[{"detail":"msgp: attempted to decode type \"int\" with method for \"map\"","status":"400","title":"Bad Request"}]}`,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequest(
				http.MethodPost,
				fmt.Sprintf("http://%s/v0.7/traces", dd.(*datadogReceiver).address),
				tc.op,
			)
			require.NoError(t, err, "Must not error when creating request")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err, "Must not error performing request")

			actual, err := io.ReadAll(resp.Body)
			require.NoError(t, errors.Join(err, resp.Body.Close()), "Must not error when reading body")

			assert.Equal(t, tc.expectContent, string(actual))
			assert.Equal(t, tc.expectCode, resp.StatusCode, "Must match the expected status code")
		})
	}
}

func TestGetDDAPIKey(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/validate", nil)

	// Test case 1: DD-API-KEY header is present
	req.Header.Set("DD-API-KEY", "api_key_value")
	apiKey := getDDAPIKey(req)
	assert.Equal(t, configopaque.String("api_key_value"), apiKey)

	// Test case 2: DD-API-KEY header is empty
	req.Header.Set("DD-API-KEY", "")
	apiKey = getDDAPIKey(req)
	assert.Equal(t, configopaque.String(""), apiKey)

	// Test case 3: DD-API-KEY header is not present, but DD-API-KEY query parameter is present
	req.Header.Del("DD-API-KEY")
	req.URL.RawQuery = "DD-API-KEY=query_param_value"
	apiKey = getDDAPIKey(req)
	assert.Equal(t, configopaque.String("query_param_value"), apiKey)

	// Test case 4: DD-API-KEY header and DD-API-KEY query parameter are not present, but api_key query parameter is present
	req.URL.RawQuery = "api_key=query_param_value"
	apiKey = getDDAPIKey(req)
	assert.Equal(t, configopaque.String("query_param_value"), apiKey)

	// Test case 5: No API key is present
	req.URL.RawQuery = ""
	apiKey = getDDAPIKey(req)
	assert.Equal(t, configopaque.String(""), apiKey)
}
