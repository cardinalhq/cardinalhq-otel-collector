package newrelicreceiver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestNewRelicReceiver_Lifecycle(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	cfg.(*Config).Endpoint = "localhost:0"
	nrr, err := factory.CreateLogs(context.Background(), receivertest.NewNopSettings(), cfg, consumertest.NewNop())
	assert.NoError(t, err, "Receiver should be created")

	err = nrr.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err, "Server should start")

	err = nrr.Shutdown(context.Background())
	assert.NoError(t, err, "Server should stop")
}

func TestNewRelicServer(t *testing.T) {
    cfg := createDefaultConfig().(*Config)
    cfg.Endpoint = "localhost:8126" // Use a specific port
    cfg.APIKey = "testing"          // Set the required API key
    nr, err := newNewRelicReceiver(cfg, receivertest.NewNopSettings())
    require.NoError(t, err, "Must not error when creating receiver")

    nr.nextLogConsumer = consumertest.NewNop()

    ctx, cancel := context.WithCancel(context.Background())
    t.Cleanup(cancel)

    require.NoError(t, nr.Start(ctx, componenttest.NewNopHost()))
    t.Cleanup(func() {
        require.NoError(t, nr.Shutdown(ctx), "Must not error shutting down")
    })

    // Test cases
    for _, tc := range []struct {
        name          string
        op            io.Reader
        apiKeyHeader  string
        expectCode    int
        expectContent string
    }{
        {
            name:          "invalid data",
            op:            strings.NewReader("{"), // Invalid JSON payload
            apiKeyHeader:  "testing",             // Provide a valid API key
            expectCode:    http.StatusBadRequest, // Expect 400 for invalid JSON
            expectContent: "Invalid payload",     // Adjust expected content to match implementation
        },
        {
            name:          "unauthorized request",
            op:            strings.NewReader("{}"), // Valid JSON payload
            apiKeyHeader:  "",                      // Missing API key
            expectCode:    http.StatusUnauthorized, // Expect 401 Unauthorized
            expectContent: "Unauthorized",          // The expected error message
        },
    } {
        tc := tc
        t.Run(tc.name, func(t *testing.T) {
            req, err := http.NewRequest(
                http.MethodPost,
                fmt.Sprintf("http://%s/logs", cfg.Endpoint),
                tc.op,
            )
            require.NoError(t, err, "Must not error when creating request")

            if tc.apiKeyHeader != "" {
                req.Header.Set("X-Insert-Key", tc.apiKeyHeader) // Set the API key if provided
            }

            resp, err := http.DefaultClient.Do(req)
            require.NoError(t, err, "Must not error performing request")

            actual, err := io.ReadAll(resp.Body)
            require.NoError(t, err)

            assert.Equal(t, tc.expectContent, strings.TrimSpace(string(actual)))
            assert.Equal(t, tc.expectCode, resp.StatusCode, "Must match the expected status code")
        })
    }
}

func TestGetNewRelicAPIKey(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/validate", nil)

	// Test case 1: X-Insert-Key header is present
	req.Header.Set("X-Insert-Key", "api_key_value")
	apiKey := GetNewRelicAPIKey(req) // Ensure the function is exported
	assert.Equal(t, "api_key_value", apiKey)

	// Test case 2: X-Insert-Key header is empty
	req.Header.Set("X-Insert-Key", "")
	apiKey = GetNewRelicAPIKey(req)
	assert.Equal(t, "", apiKey)

	// Test case 3: X-Insert-Key header is not present
	req.Header.Del("X-Insert-Key")
	apiKey = GetNewRelicAPIKey(req)
	assert.Equal(t, "", apiKey)
}
