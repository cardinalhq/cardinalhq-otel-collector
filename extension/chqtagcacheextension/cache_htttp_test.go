package chqtagcacheextension

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/confighttp"
)

type MockRoundTripper struct {
	response    *http.Response
	returnError error
}

func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.response, m.returnError
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
			expectedError: "Failed to fetch tags, error code: 404",
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
			tags:          nil,
			expectedError: "Failed to put tags, error code: 404",
		},
		{
			name:          "error",
			tripper:       &MockRoundTripper{returnError: errors.New("Post")},
			tags:          nil,
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
			expectedError: "json: unsupported type",
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
