// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package finalizenotify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Endpoint carries the base URL of the target lakerunner deployment plus the
// producer credentials the receiver's audience middleware requires.
type Endpoint struct {
	// BaseURL is the origin of the lakerunner admin service — e.g.
	// https://lakerunner.example.com.
	BaseURL string
	// Identity is the canonical producer identity persisted on every
	// finalization row. Matches the receiver's persisted CHECK shape.
	Identity string
	// Token is the shared secret whose SHA-256 hex matches the receiver's
	// LAKERUNNER_INTAKE_FINALIZATION_AUDIENCE_KEYS allowlist.
	Token string
}

// Result classifies the outcome of one POST.
type Result string

const (
	// ResultCreated: the receiver newly persisted the row.
	ResultCreated Result = "created"
	// ResultReplayed: the receiver had a bytewise-identical row from the
	// same producer. Idempotent, safe.
	ResultReplayed Result = "replayed"
)

// ErrorCode names a non-retryable / retryable failure mode. Callers decide
// backoff via IsRetryable.
type ErrorCode string

const (
	ErrCodeUnconfigured        ErrorCode = "UNCONFIGURED"
	ErrCodeCredentialsInvalid  ErrorCode = "CREDENTIALS_INVALID"
	ErrCodeConflict            ErrorCode = "CONFLICT"
	ErrCodeRuntimeUnavailable  ErrorCode = "RUNTIME_UNAVAILABLE"
	ErrCodeBadResponse         ErrorCode = "BAD_RESPONSE"
	ErrCodeRequestInvalid      ErrorCode = "REQUEST_INVALID"
	ErrCodeTransport           ErrorCode = "TRANSPORT"
	ErrCodeTimeout             ErrorCode = "TIMEOUT"
)

// Error carries the classified failure.
type Error struct {
	Code       ErrorCode
	Message    string
	HTTPStatus int
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// IsRetryable is the classifier a worker uses to decide whether to back off
// (transient failures) or abort (permanent).
func IsRetryable(code ErrorCode) bool {
	switch code {
	case ErrCodeTimeout, ErrCodeTransport, ErrCodeRuntimeUnavailable:
		return true
	default:
		return false
	}
}

// Doer is the tiny fetch interface the client needs. *http.Client satisfies
// it; tests provide their own implementation.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Publisher POSTs one authenticated finalization envelope to the receiver.
// The publisher is stateless — no queue, no batching — so callers can share
// one instance across many concurrent goroutines. Retry + queue behaviour
// belongs to the heartbeat processor that owns aligned-window state; keeping
// the publisher stateless means it does not need its own persistence.
type Publisher struct {
	client   Doer
	endpoint Endpoint
	timeout  time.Duration
}

// New builds a Publisher. Passing http.DefaultClient is fine; production
// deployments should supply a client configured for the target base URL.
func New(client Doer, endpoint Endpoint, timeout time.Duration) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("finalizenotify: http client is required")
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Publisher{client: client, endpoint: endpoint, timeout: timeout}, nil
}

// Validate returns nil when the endpoint is complete enough to POST.
func (e Endpoint) Validate() error {
	if strings.TrimSpace(e.BaseURL) == "" {
		return errors.New("finalizenotify: endpoint BaseURL is required")
	}
	if err := ValidateProducerIdentity(e.Identity); err != nil {
		return err
	}
	if strings.TrimSpace(e.Token) == "" {
		return errors.New("finalizenotify: endpoint Token is required")
	}
	return nil
}

// Publish sends one finalization request and returns the receiver's classified
// outcome. The caller MUST have already validated req and computed FrontierHash.
func (p *Publisher) Publish(ctx context.Context, req FinalizationRequest) (Result, error) {
	body, err := Marshal(req)
	if err != nil {
		return "", &Error{Code: ErrCodeRequestInvalid, Message: err.Error()}
	}
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	url := strings.TrimRight(p.endpoint.BaseURL, "/") + "/internal/finalize/v1"
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", &Error{Code: ErrCodeRequestInvalid, Message: err.Error()}
	}
	httpReq.Header.Set("Content-Type", MediaType)
	httpReq.Header.Set("Accept", MediaType)
	httpReq.Header.Set("Authorization", "Bearer "+p.endpoint.Token)
	httpReq.Header.Set(IdentityHeader, p.endpoint.Identity)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return "", &Error{Code: ErrCodeTimeout, Message: "finalization request timed out"}
		}
		return "", &Error{Code: ErrCodeTransport, Message: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))

	switch resp.StatusCode {
	case http.StatusAccepted:
		// Receiver returns application/vnd.cardinal.finalize+json;version=1
		// with {"status": "created" | "replayed", "persisted_frontier_hash": ...}.
		if bytes.Contains(respBody, []byte(`"created"`)) {
			return ResultCreated, nil
		}
		if bytes.Contains(respBody, []byte(`"replayed"`)) {
			return ResultReplayed, nil
		}
		return "", &Error{Code: ErrCodeBadResponse, Message: "receiver returned 202 without a known status"}
	case http.StatusUnauthorized:
		return "", &Error{Code: ErrCodeCredentialsInvalid, Message: extractMessage(respBody, "invalid finalization credentials"), HTTPStatus: resp.StatusCode}
	case http.StatusConflict:
		return "", &Error{Code: ErrCodeConflict, Message: extractMessage(respBody, "finalization conflict"), HTTPStatus: resp.StatusCode}
	case http.StatusServiceUnavailable:
		msg := extractMessage(respBody, "receiver unavailable")
		if strings.Contains(strings.ToLower(msg), "audience is not configured") {
			return "", &Error{Code: ErrCodeUnconfigured, Message: msg, HTTPStatus: resp.StatusCode}
		}
		return "", &Error{Code: ErrCodeRuntimeUnavailable, Message: msg, HTTPStatus: resp.StatusCode}
	case http.StatusBadRequest, http.StatusUnsupportedMediaType:
		return "", &Error{Code: ErrCodeRequestInvalid, Message: extractMessage(respBody, "receiver rejected the request"), HTTPStatus: resp.StatusCode}
	default:
		if resp.StatusCode >= 500 {
			return "", &Error{Code: ErrCodeTransport, Message: fmt.Sprintf("receiver returned %d", resp.StatusCode), HTTPStatus: resp.StatusCode}
		}
		return "", &Error{Code: ErrCodeBadResponse, Message: fmt.Sprintf("receiver returned unexpected status %d", resp.StatusCode), HTTPStatus: resp.StatusCode}
	}
}

// extractMessage pulls "message":"..." out of a small JSON error envelope
// without a full parse — the response bodies here are all under 8 KiB and
// have a fixed adminapi shape.
func extractMessage(body []byte, fallback string) string {
	if len(body) == 0 {
		return fallback
	}
	idx := bytes.Index(body, []byte(`"message"`))
	if idx < 0 {
		return fallback
	}
	rest := body[idx:]
	quote := bytes.IndexByte(rest, ':')
	if quote < 0 {
		return fallback
	}
	value := rest[quote+1:]
	open := bytes.IndexByte(value, '"')
	if open < 0 {
		return fallback
	}
	value = value[open+1:]
	close := bytes.IndexByte(value, '"')
	if close < 0 {
		return fallback
	}
	return string(value[:close])
}
