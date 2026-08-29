// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package finalizenotify

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeDoer struct {
	fn func(*http.Request) (*http.Response, error)
}

func (f *fakeDoer) Do(req *http.Request) (*http.Response, error) { return f.fn(req) }

func mkResponse(status int, body string, contentType string) *http.Response {
	if contentType == "" {
		contentType = MediaType
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
		Header:     http.Header{"Content-Type": []string{contentType}},
	}
}

const testEndpointIdentity = "customer-intake.chq-saas.v1"

func testEndpoint(url string) Endpoint {
	return Endpoint{BaseURL: url, Identity: testEndpointIdentity, Token: "test-token"}
}

func TestEndpointValidateRejectsBadShape(t *testing.T) {
	// Missing BaseURL entirely.
	assert.Error(t, Endpoint{Identity: testEndpointIdentity, Token: "t"}.Validate())
	// Missing token.
	assert.Error(t, Endpoint{BaseURL: "https://x", Identity: testEndpointIdentity}.Validate())
	// Bad identity shape.
	assert.Error(t, Endpoint{BaseURL: "https://x", Identity: "BAD", Token: "t"}.Validate())
	// Well-formed https endpoint accepted.
	assert.NoError(t, Endpoint{BaseURL: "https://x", Identity: testEndpointIdentity, Token: "t"}.Validate())
}

func TestEndpointValidateRequiresHTTPS(t *testing.T) {
	// http:// is rejected by default: production must not exfiltrate the
	// bearer token in plaintext.
	err := Endpoint{BaseURL: "http://x", Identity: testEndpointIdentity, Token: "t"}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https")

	// The AllowInsecureBaseURL escape hatch lets local dev use http://.
	assert.NoError(t, Endpoint{BaseURL: "http://x", Identity: testEndpointIdentity, Token: "t", AllowInsecureBaseURL: true}.Validate())

	// https:// is always accepted regardless of the boolean.
	assert.NoError(t, Endpoint{BaseURL: "https://x", Identity: testEndpointIdentity, Token: "t"}.Validate())
	assert.NoError(t, Endpoint{BaseURL: "https://x", Identity: testEndpointIdentity, Token: "t", AllowInsecureBaseURL: true}.Validate())

	// Non-http(s) schemes are rejected even when the escape hatch is set —
	// the hatch is only for plaintext dev, not for arbitrary schemes.
	err = Endpoint{BaseURL: "ftp://x", Identity: testEndpointIdentity, Token: "t"}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https")
	err = Endpoint{BaseURL: "ftp://x", Identity: testEndpointIdentity, Token: "t", AllowInsecureBaseURL: true}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https")
}

func TestPublishCreated(t *testing.T) {
	var seen *http.Request
	doer := &fakeDoer{fn: func(r *http.Request) (*http.Response, error) {
		seen = r
		return mkResponse(http.StatusAccepted, `{"status":"created","persisted_frontier_hash":"aaaa"}`, ""), nil
	}}
	pub, err := New(doer, testEndpoint("https://lakerunner.example.com"), time.Second)
	require.NoError(t, err)
	req := baseRequest(t)
	req.FrontierHash, _ = ComputeFrontierHash(req, testEndpointIdentity)
	result, err := pub.Publish(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, ResultCreated, result)
	require.NotNil(t, seen)
	assert.Equal(t, "POST", seen.Method)
	assert.Equal(t, "https://lakerunner.example.com/internal/finalize/v1", seen.URL.String())
	assert.Equal(t, MediaType, seen.Header.Get("Content-Type"))
	assert.Equal(t, "Bearer test-token", seen.Header.Get("Authorization"))
	assert.Equal(t, testEndpointIdentity, seen.Header.Get(IdentityHeader))
}

func TestPublishReplayed(t *testing.T) {
	doer := &fakeDoer{fn: func(r *http.Request) (*http.Response, error) {
		return mkResponse(http.StatusAccepted, `{"status":"replayed","persisted_frontier_hash":"aaaa"}`, ""), nil
	}}
	pub, _ := New(doer, testEndpoint("https://lakerunner.example.com"), time.Second)
	req := baseRequest(t)
	req.FrontierHash, _ = ComputeFrontierHash(req, testEndpointIdentity)
	result, err := pub.Publish(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, ResultReplayed, result)
}

func TestPublishMapsStatusCodesToErrors(t *testing.T) {
	cases := map[int]ErrorCode{
		http.StatusUnauthorized:        ErrCodeCredentialsInvalid,
		http.StatusConflict:            ErrCodeConflict,
		http.StatusBadRequest:          ErrCodeRequestInvalid,
		http.StatusUnsupportedMediaType: ErrCodeRequestInvalid,
		http.StatusInternalServerError: ErrCodeTransport,
	}
	for status, code := range cases {
		t.Run(http.StatusText(status), func(t *testing.T) {
			doer := &fakeDoer{fn: func(r *http.Request) (*http.Response, error) {
				return mkResponse(status, `{"code":"X","message":"receiver rejected"}`, ""), nil
			}}
			pub, _ := New(doer, testEndpoint("https://x"), time.Second)
			req := baseRequest(t)
			req.FrontierHash, _ = ComputeFrontierHash(req, testEndpointIdentity)
			_, err := pub.Publish(context.Background(), req)
			var pe *Error
			require.True(t, errors.As(err, &pe))
			assert.Equal(t, code, pe.Code)
		})
	}
}

func TestPublishDistinguishesUnconfiguredFromRuntimeUnavailable(t *testing.T) {
	unconfigured := &fakeDoer{fn: func(r *http.Request) (*http.Response, error) {
		return mkResponse(http.StatusServiceUnavailable, `{"code":"NOT_IMPLEMENTED","message":"finalization audience is not configured"}`, ""), nil
	}}
	runtimeGone := &fakeDoer{fn: func(r *http.Request) (*http.Response, error) {
		return mkResponse(http.StatusServiceUnavailable, `{"code":"NOT_IMPLEMENTED","message":"finalization receiver is not configured"}`, ""), nil
	}}
	req := baseRequest(t)
	req.FrontierHash, _ = ComputeFrontierHash(req, testEndpointIdentity)

	pubA, _ := New(unconfigured, testEndpoint("https://x"), time.Second)
	_, err := pubA.Publish(context.Background(), req)
	var pe *Error
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, ErrCodeUnconfigured, pe.Code)
	assert.False(t, IsRetryable(pe.Code))

	pubB, _ := New(runtimeGone, testEndpoint("https://x"), time.Second)
	_, err = pubB.Publish(context.Background(), req)
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, ErrCodeRuntimeUnavailable, pe.Code)
	assert.True(t, IsRetryable(pe.Code))
}

func TestPublishTransportErrorIsRetryable(t *testing.T) {
	doer := &fakeDoer{fn: func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("connect ECONNREFUSED")
	}}
	pub, _ := New(doer, testEndpoint("https://x"), time.Second)
	req := baseRequest(t)
	req.FrontierHash, _ = ComputeFrontierHash(req, testEndpointIdentity)
	_, err := pub.Publish(context.Background(), req)
	var pe *Error
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, ErrCodeTransport, pe.Code)
	assert.True(t, IsRetryable(pe.Code))
}

func TestPublishTimeoutIsRetryable(t *testing.T) {
	doer := &fakeDoer{fn: func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	}}
	pub, _ := New(doer, testEndpoint("https://x"), 10*time.Millisecond)
	req := baseRequest(t)
	req.FrontierHash, _ = ComputeFrontierHash(req, testEndpointIdentity)
	_, err := pub.Publish(context.Background(), req)
	var pe *Error
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, ErrCodeTimeout, pe.Code)
	assert.True(t, IsRetryable(pe.Code))
}

func TestPublishCallerCancellationIsNonRetryable(t *testing.T) {
	// A real httptest server whose handler blocks until the caller cancels
	// the context — this exercises the true cancellation path through the
	// http transport rather than a synthesised error.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)

	// AllowInsecureBaseURL because httptest.NewServer is http://.
	pub, err := New(srv.Client(), Endpoint{
		BaseURL:              srv.URL,
		Identity:             testEndpointIdentity,
		Token:                "test-token",
		AllowInsecureBaseURL: true,
	}, 5*time.Second)
	require.NoError(t, err)

	req := baseRequest(t)
	req.FrontierHash, _ = ComputeFrontierHash(req, testEndpointIdentity)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel shortly after Publish starts so the request is in-flight.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err = pub.Publish(ctx, req)
	var pe *Error
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, ErrCodeCancelled, pe.Code)
	assert.False(t, IsRetryable(pe.Code), "cancellation must not be retried — the caller asked us to stop")
}

func TestPublishRejectsMalformedRequestBeforeSending(t *testing.T) {
	sent := false
	doer := &fakeDoer{fn: func(*http.Request) (*http.Response, error) {
		sent = true
		return mkResponse(http.StatusAccepted, `{"status":"created"}`, ""), nil
	}}
	pub, _ := New(doer, testEndpoint("https://x"), time.Second)
	req := baseRequest(t)
	req.FrontierHash = "not-hex"
	_, err := pub.Publish(context.Background(), req)
	var pe *Error
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, ErrCodeRequestInvalid, pe.Code)
	assert.False(t, sent, "malformed request must not reach the wire")
}

func TestExtractMessageFallback(t *testing.T) {
	// No JSON body → fallback used.
	assert.Equal(t, "fallback", extractMessage([]byte(""), "fallback"))
	// Body without message field → fallback used.
	assert.Equal(t, "fallback", extractMessage([]byte(`{"code":"X"}`), "fallback"))
	// Body with message field → extracted.
	assert.Equal(t, "hello", extractMessage([]byte(`{"code":"X","message":"hello"}`), "fallback"))
	// Body with embedded quotes → captures up to the first close.
	got := extractMessage([]byte(`{"code":"X","message":"say \"hi\""}`), "fallback")
	assert.True(t, strings.HasPrefix(got, "say "))
}
