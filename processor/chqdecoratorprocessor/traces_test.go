// Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chqdecoratorprocessor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/spantagger"
	"go.uber.org/zap"
)

func TestNewTrace(t *testing.T) {
	sp := &spansProcessor{
		logger: zap.NewNop(),
		sentFingerprints: fingerprintTracker{
			fingerprints: make(map[uint64]struct{}),
		},
		telemetry: &processorTelemetry{},
	}

	fingerprint := uint64(1234567890)

	// First call with a new fingerprint should return true
	if !sp.newTrace(fingerprint) {
		t.Error("expected newTrace to return true")
	}

	// Second call with the same fingerprint should return false
	if sp.newTrace(fingerprint) {
		t.Error("expected newTrace to return false")
	}
}
func TestSendGraph(t *testing.T) {
	// Create a mock server to receive the HTTP request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request method and URL
		if r.Method != http.MethodPost {
			t.Errorf("expected POST request, got %s", r.Method)
		}
		if r.URL.String() != "/graph" {
			t.Errorf("expected URL /graph, got %s", r.URL.String())
		}

		// Verify the request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		expectedBody := `{"fingerprint":1234567890}`
		if string(body) != expectedBody {
			t.Errorf("expected request body %s, got %s", expectedBody, string(body))
		}

		// Send a successful response
		w.WriteHeader(http.StatusOK)
	}))

	// Create a spansProcessor instance with the mock server URL
	sp := &spansProcessor{
		logger:    zap.NewNop(),
		telemetry: &processorTelemetry{},
		graphURL:  server.URL + "/graph",
	}

	// Create a mock graph
	graph := &spantagger.Graph{
		Fingerprint: 1234567890,
	}

	// Call the sendGraph function
	err := sp.sendGraph(context.Background(), graph)
	if err != nil {
		t.Errorf("sendGraph failed: %v", err)
	}

	// Close the mock server
	server.Close()
}
