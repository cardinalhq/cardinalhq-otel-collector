// Copyright 2024-2025 CardinalHQ, Inc
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

package chqtagcacheextension

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/otel/sdk/metric"
)

type MockRoundTripper struct {
	response    *http.Response
	returnError error
}

func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.response, m.returnError
}

func mockTelemetry(chq *CHQTagcacheExtension) {
	meter := metric.NewMeterProvider()
	ic, _ := meter.Meter("test").Int64Counter("items_written_temp")

	chq.cacheGets = ic
	chq.cacheMisses = ic
	chq.cachePuts = ic
}

func TestCHQTagcacheExtension_FetchTags(t *testing.T) {
	tests := []struct {
		name          string
		tripper       *MockRoundTripper
		expectedTags  any
		expectedError string
	}{
		{
			name: "valid",
			tripper: &MockRoundTripper{
				response: &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(bytes.NewBufferString(`{"tags":[{"name":"tag1","value":"value1"},{"name":"tag2","value":"value2"}]}`)),
				},
			},
			expectedTags: []Tag{
				{Name: "tag1", Value: "value1"},
				{Name: "tag2", Value: "value2"},
			},
			expectedError: "",
		},
		{
			name: "invalid response",
			tripper: &MockRoundTripper{
				response: &http.Response{
					StatusCode: http.StatusNotFound,
					Status:     "404 Not Found",
				},
			},
			expectedTags:  nil,
			expectedError: "failed to fetch tags, error code: 404",
		},
		{
			name: "invalid JSON",
			tripper: &MockRoundTripper{
				response: &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(bytes.NewBufferString(`invalid json`)),
				},
			},
			expectedTags:  nil,
			expectedError: "invalid character",
		},
		{
			name:          "error",
			tripper:       &MockRoundTripper{returnError: errors.New("Get")},
			expectedTags:  nil,
			expectedError: "Get",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock the HTTP response
			mockClient := &http.Client{
				Transport: tt.tripper,
			}

			chq := &CHQTagcacheExtension{
				config: &Config{
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://example.com",
					},
				},
				httpClient: mockClient,
			}
			mockTelemetry(chq)

			tags, err := chq.tagFetcher("example-key")

			require.Equal(t, tt.expectedTags, tags)
			if tt.expectedError == "" {
				require.NoError(t, err)
			} else if err != nil {
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.Fail(t, "expected error but got nil")
			}
		})
	}
}

func TestCHQTagcacheExtension_PutTags(t *testing.T) {
	tests := []struct {
		name          string
		tripper       *MockRoundTripper
		tags          any
		expectedError string
	}{
		{
			name: "valid",
			tripper: &MockRoundTripper{
				response: &http.Response{
					StatusCode: http.StatusAccepted,
					Status:     "202 Accepted",
				},
			},
			tags: []Tag{
				{Name: "tag1", Value: "value1"},
				{Name: "tag2", Value: "value2"},
			},
			expectedError: "",
		},
		{
			name: "invalid response",
			tripper: &MockRoundTripper{
				response: &http.Response{
					StatusCode: http.StatusNotFound,
					Status:     "404 Not Found",
				},
			},
			tags:          []Tag{},
			expectedError: "failed to put tags, error code: 404",
		},
		{
			name:          "error",
			tripper:       &MockRoundTripper{returnError: errors.New("Post")},
			tags:          []Tag{},
			expectedError: "Post",
		},
		{
			name: "invalid JSON",
			tripper: &MockRoundTripper{
				response: &http.Response{
					StatusCode: http.StatusAccepted,
					Status:     "202 Accepted",
				},
			},
			tags:          make(chan int),
			expectedError: "invalid type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &http.Client{
				Transport: tt.tripper,
			}

			chq := &CHQTagcacheExtension{
				config: &Config{
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://example.com",
					},
				},
				httpClient: mockClient,
			}
			mockTelemetry(chq)

			err := chq.tagPutter("example-key", tt.tags)

			if tt.expectedError == "" {
				require.NoError(t, err)
			} else if err != nil {
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.Fail(t, "expected error but got nil")
			}
		})
	}
}
