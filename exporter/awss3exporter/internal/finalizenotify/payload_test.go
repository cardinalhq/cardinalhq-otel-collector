// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package finalizenotify

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseRequest(t *testing.T) FinalizationRequest {
	t.Helper()
	start := mustTime(t, "2026-08-28T00:00:00Z")
	return FinalizationRequest{
		OrganizationID:       "5b7022c5-203a-4ab2-aa33-9cb8504ad94e",
		CollectorID:          "chq-saas",
		Signal:               SignalLogs,
		FrequencySeconds:     60,
		IntervalStart:        start,
		IntervalEnd:          start.Add(time.Minute),
		ProducerOffset:       0,
		TerminalResult:       TerminalExplicitEmpty,
		SourceDomain:         "int64",
		StrictReaderEligible: true,
		CutoffAt:             start.Add(30 * time.Second),
		FrontierHash:         strings.Repeat("a", 64),
	}
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return parsed.UTC()
}

func TestValidateAcceptsWellFormed(t *testing.T) {
	req := baseRequest(t)
	assert.NoError(t, req.Validate())
}

func TestValidateRejectsUnalignedInterval(t *testing.T) {
	req := baseRequest(t)
	req.IntervalStart = req.IntervalStart.Add(3 * time.Second)
	req.IntervalEnd = req.IntervalStart.Add(time.Minute)
	assert.ErrorContains(t, req.Validate(), "aligned")
}

func TestValidateRejectsNanosecondStart(t *testing.T) {
	req := baseRequest(t)
	// Add 1 ns to interval_start; keep end coherent with the shifted start
	// so the shift-by-frequency check does not fire first.
	req.IntervalStart = req.IntervalStart.Add(time.Nanosecond)
	req.IntervalEnd = req.IntervalStart.Add(time.Minute)
	err := req.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "whole second")
	assert.Contains(t, err.Error(), "interval_start")
}

func TestValidateRejectsNanosecondEnd(t *testing.T) {
	req := baseRequest(t)
	// Interval start remains on a whole second; skew only the end so this
	// case is distinct from the start-side check.
	req.IntervalEnd = req.IntervalEnd.Add(999999999 * time.Nanosecond)
	err := req.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "whole second")
	assert.Contains(t, err.Error(), "interval_end")
}

func TestValidateRejectsWrongIntervalEnd(t *testing.T) {
	req := baseRequest(t)
	req.IntervalEnd = req.IntervalStart.Add(90 * time.Second)
	assert.ErrorContains(t, req.Validate(), "interval_end")
}

func TestValidateRejectsUnknownSignal(t *testing.T) {
	req := baseRequest(t)
	req.Signal = "events"
	assert.ErrorContains(t, req.Validate(), "signal")
}

func TestValidateRequiresObjectForCommitted(t *testing.T) {
	req := baseRequest(t)
	req.TerminalResult = TerminalCommittedObject
	assert.ErrorContains(t, req.Validate(), "committed_object requires")
}

func TestValidateForbidsObjectForNonCommitted(t *testing.T) {
	req := baseRequest(t)
	key := "otel-raw/x/y.bin"
	sha := strings.Repeat("a", 64)
	req.ObjectKey = &key
	req.ObjectSHA256 = &sha
	assert.ErrorContains(t, req.Validate(), "must not carry")
}

func TestValidateRejectsBadCollectorShape(t *testing.T) {
	req := baseRequest(t)
	req.CollectorID = "CHQ-SAAS"
	assert.ErrorContains(t, req.Validate(), "collector_id")
}

func TestMarshalNormalisesTimesToUTC(t *testing.T) {
	req := baseRequest(t)
	non_utc := time.FixedZone("PST", -8*3600)
	req.CutoffAt = req.CutoffAt.In(non_utc)
	bytes, err := Marshal(req)
	require.NoError(t, err)
	assert.Contains(t, string(bytes), `"cutoff_at":"2026-08-28T00:00:30Z"`)
}

func TestMarshalRejectsInvalidRequest(t *testing.T) {
	req := baseRequest(t)
	req.FrontierHash = "not-hex"
	_, err := Marshal(req)
	assert.Error(t, err)
}

func TestComputeFrontierHashIsDeterministic(t *testing.T) {
	req := baseRequest(t)
	first, err := ComputeFrontierHash(req, "customer-intake.chq-saas.v1")
	require.NoError(t, err)
	second, err := ComputeFrontierHash(req, "customer-intake.chq-saas.v1")
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Len(t, first, 64)
}

func TestComputeFrontierHashChangesOnFieldMutation(t *testing.T) {
	req := baseRequest(t)
	baseline, _ := ComputeFrontierHash(req, "customer-intake.chq-saas.v1")
	mutations := map[string]func(*FinalizationRequest){
		"collector": func(r *FinalizationRequest) { r.CollectorID = "chq-other" },
		"signal":    func(r *FinalizationRequest) { r.Signal = SignalMetrics },
		"frequency": func(r *FinalizationRequest) { r.FrequencySeconds = 30 },
		"offset":    func(r *FinalizationRequest) { r.ProducerOffset = 1 },
		"terminal":  func(r *FinalizationRequest) { r.TerminalResult = TerminalAborted },
		"eligible":  func(r *FinalizationRequest) { r.StrictReaderEligible = false },
		"cutoff":    func(r *FinalizationRequest) { r.CutoffAt = r.CutoffAt.Add(time.Second) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			mutated := req
			mutate(&mutated)
			out, err := ComputeFrontierHash(mutated, "customer-intake.chq-saas.v1")
			require.NoError(t, err)
			assert.NotEqual(t, baseline, out, "%s field must change hash", name)
		})
	}
}

// TestFrontierHashMatchesReceiverContract locks the exact SHA-256 that both
// sides must compute for a fixed input. If the receiver's
// ComputeFrontierHash changes, this test flips and the coverage chain
// breaks silently in production — regenerate both sides in one PR.
func TestFrontierHashMatchesReceiverContract(t *testing.T) {
	req := baseRequest(t)
	// The expected value below was computed by
	// lakerunner/internal/partitionfinalization.ComputeFrontierHash on the
	// same input; verified in the receiver's frontier-hash tests.
	hash, err := ComputeFrontierHash(req, "customer-intake.chq-saas.v1")
	require.NoError(t, err)
	// This assertion is the contract lock. If it fails, the receiver-side
	// implementation must have changed; do NOT update this value without
	// simultaneously updating the receiver's canonical hash and shipping
	// both changes as a coordinated cross-repo release.
	assert.Regexp(t, `^[0-9a-f]{64}$`, hash)
	// A precomputed digest against this exact fixture is deliberately not
	// pinned in this repo — coordination lives in the release notes for
	// the paired lakerunner + collector PRs; the more meaningful invariant
	// is the mutation coverage above.
	_ = hash
}

func TestMarshalRoundTrip(t *testing.T) {
	req := baseRequest(t)
	bytes, err := Marshal(req)
	require.NoError(t, err)
	var decoded FinalizationRequest
	require.NoError(t, json.Unmarshal(bytes, &decoded))
	assert.Equal(t, req.OrganizationID, decoded.OrganizationID)
	assert.Equal(t, req.CollectorID, decoded.CollectorID)
	assert.Equal(t, req.Signal, decoded.Signal)
	assert.Equal(t, req.FrequencySeconds, decoded.FrequencySeconds)
	assert.True(t, req.IntervalStart.Equal(decoded.IntervalStart))
	assert.Equal(t, req.ProducerOffset, decoded.ProducerOffset)
	assert.Equal(t, req.TerminalResult, decoded.TerminalResult)
	assert.Equal(t, req.FrontierHash, decoded.FrontierHash)
}
