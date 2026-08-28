// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package finalizenotify

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
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
	assert.Error(t, Endpoint{Identity: "x", Token: "t"}.Validate())
	assert.Error(t, Endpoint{BaseURL: "http://x", Token: "t"}.Validate())
	assert.Error(t, Endpoint{BaseURL: "http://x", Identity: "BAD", Token: "t"}.Validate())
	assert.Error(t, Endpoint{BaseURL: "http://x", Identity: testEndpointIdentity}.Validate())
	assert.NoError(t, Endpoint{BaseURL: "http://x", Identity: testEndpointIdentity, Token: "t"}.Validate())
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
