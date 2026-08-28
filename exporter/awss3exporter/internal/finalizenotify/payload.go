// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package finalizenotify carries an authenticated per-partition finalization
// event from the customer-intake collector to lakerunner's coverage-ledger
// receiver (introduced by lakerunner PR #1406).
//
// The wire types + canonical byte format live here so a follow-up
// heartbeat processor can build attested payloads and hand them to a
// deliverer implemented over this package's HTTP client. Keeping this
// package free of exporter- or processor-package references means both
// the awss3exporter (post-upload hook) and a future aligned-window
// heartbeat processor can consume it without an import cycle.
//
// The payload shape mirrors
// lakerunner/internal/partitionfinalization/receiver.go FinalizationRequest
// byte-for-byte. Any change to that struct MUST be mirrored here in the
// same PR or coverage will silently disagree.
package finalizenotify // import "github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter/internal/finalizenotify"

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

// TerminalResult is the closed vocabulary the receiver accepts. Emitting any
// other string is an INVALID_REQUEST on the wire, so we type-safe it here.
type TerminalResult string

const (
	// TerminalCommittedObject: the collector wrote an object to S3 and its
	// bytes are addressable by ObjectKey / ObjectSHA256.
	TerminalCommittedObject TerminalResult = "committed_object"
	// TerminalExplicitEmpty: the aligned window closed with zero records
	// from a live collector. This is the value that distinguishes an
	// authenticated empty from "we heard nothing" on the receiver side.
	TerminalExplicitEmpty TerminalResult = "explicit_empty"
	// TerminalAborted: the collector was up but declined to attest the
	// window (e.g. mid-window crash recovery, resource pressure). The
	// coverage authority treats this as terminal, non-empty, non-committed.
	TerminalAborted TerminalResult = "aborted"
)

// Signal mirrors the receiver's accepted signal set.
type Signal string

const (
	SignalLogs    Signal = "logs"
	SignalMetrics Signal = "metrics"
	SignalTraces  Signal = "traces"
)

// FinalizationRequest is the exact wire payload the receiver expects. Every
// field name and JSON tag matches lakerunner's Go struct so a byte-for-byte
// round-trip carries no information loss.
type FinalizationRequest struct {
	OrganizationID       string         `json:"organization_id"`
	CollectorID          string         `json:"collector_id"`
	Signal               Signal         `json:"signal"`
	FrequencySeconds     int64          `json:"frequency_seconds"`
	IntervalStart        time.Time      `json:"interval_start"`
	IntervalEnd          time.Time      `json:"interval_end"`
	ProducerOffset       int64          `json:"producer_offset"`
	TerminalResult       TerminalResult `json:"terminal_result"`
	ObjectKey            *string        `json:"object_key,omitempty"`
	ObjectSHA256         *string        `json:"object_sha256,omitempty"`
	SourceDomain         string         `json:"source_domain"`
	StrictReaderEligible bool           `json:"strict_reader_eligible"`
	CutoffAt             time.Time      `json:"cutoff_at"`
	PredecessorHash      *string        `json:"predecessor_hash,omitempty"`
	FrontierHash         string         `json:"frontier_hash"`
}

// MediaType matches the Content-Type the receiver requires.
const MediaType = "application/vnd.cardinal.finalize+json;version=1"

// IdentityHeader is the HTTP header carrying the producer identity paired
// with the bearer token; the receiver's audience middleware reads both.
const IdentityHeader = "X-Cardinal-Producer-Identity"

var (
	identityPattern     = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,127}$`)
	sourceDomainPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	sha256Pattern       = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Validate returns nil when req would be accepted by the receiver. Every
// receiver-side CHECK / semantic rule is mirrored here so a producer that
// validated its own payload cannot land in the receiver's INVALID_REQUEST
// branch.
func (req FinalizationRequest) Validate() error {
	if req.OrganizationID == "" {
		return fmt.Errorf("organization_id must be a UUID")
	}
	if !identityPattern.MatchString(req.CollectorID) {
		return fmt.Errorf("collector_id has invalid shape")
	}
	switch req.Signal {
	case SignalLogs, SignalMetrics, SignalTraces:
	default:
		return fmt.Errorf("signal must be one of logs, metrics, traces")
	}
	if req.FrequencySeconds <= 0 || req.FrequencySeconds > 604_800 {
		return fmt.Errorf("frequency_seconds must be in (0, 604800]")
	}
	if req.IntervalStart.IsZero() {
		return fmt.Errorf("interval_start must be a real time")
	}
	if !req.IntervalEnd.Equal(req.IntervalStart.Add(time.Duration(req.FrequencySeconds) * time.Second)) {
		return fmt.Errorf("interval_end must equal interval_start + frequency_seconds")
	}
	if req.IntervalStart.UTC().Unix()%req.FrequencySeconds != 0 {
		return fmt.Errorf("interval_start must be aligned to frequency_seconds")
	}
	if req.ProducerOffset < 0 {
		return fmt.Errorf("producer_offset must be non-negative")
	}
	switch req.TerminalResult {
	case TerminalCommittedObject:
		if req.ObjectKey == nil || *req.ObjectKey == "" {
			return fmt.Errorf("committed_object requires object_key")
		}
		if req.ObjectSHA256 == nil || !sha256Pattern.MatchString(*req.ObjectSHA256) {
			return fmt.Errorf("committed_object requires object_sha256 as lowercase SHA-256")
		}
	case TerminalExplicitEmpty, TerminalAborted:
		if req.ObjectKey != nil || req.ObjectSHA256 != nil {
			return fmt.Errorf("non-committed_object results must not carry object_key or object_sha256")
		}
	default:
		return fmt.Errorf("terminal_result must be one of committed_object, explicit_empty, aborted")
	}
	if !sourceDomainPattern.MatchString(req.SourceDomain) {
		return fmt.Errorf("source_domain has invalid shape")
	}
	if req.CutoffAt.IsZero() {
		return fmt.Errorf("cutoff_at must be a real time")
	}
	if req.PredecessorHash != nil && !sha256Pattern.MatchString(*req.PredecessorHash) {
		return fmt.Errorf("predecessor_hash must be lowercase SHA-256")
	}
	if !sha256Pattern.MatchString(req.FrontierHash) {
		return fmt.Errorf("frontier_hash must be lowercase SHA-256")
	}
	return nil
}

// ValidateProducerIdentity mirrors the receiver's identity CHECK so a
// producer can reject its own configuration before it ever attempts a POST.
func ValidateProducerIdentity(identity string) error {
	if !identityPattern.MatchString(identity) {
		return fmt.Errorf("producer_identity has invalid shape")
	}
	return nil
}

// Marshal returns the exact JSON envelope the receiver expects. Times are
// normalised to UTC RFC3339Nano so producers running with a non-UTC local
// clock cannot silently emit non-canonical bytes.
func Marshal(req FinalizationRequest) ([]byte, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	// Copy times to UTC in the encoded form so json.Marshal emits stable
	// bytes independent of the producer's local clock zone.
	toEncode := req
	toEncode.IntervalStart = req.IntervalStart.UTC()
	toEncode.IntervalEnd = req.IntervalEnd.UTC()
	toEncode.CutoffAt = req.CutoffAt.UTC()
	return json.Marshal(toEncode)
}

// ComputeFrontierHash mirrors the receiver's ComputeFrontierHash exactly.
// A producer builds the request, calls this to fill FrontierHash, then POSTs.
// The receiver recomputes internally and rejects any drift.
func ComputeFrontierHash(req FinalizationRequest, producerIdentity string) (string, error) {
	if err := ValidateProducerIdentity(producerIdentity); err != nil {
		return "", err
	}
	commit := map[string]any{
		"organization_id":        req.OrganizationID,
		"collector_id":           req.CollectorID,
		"signal":                 string(req.Signal),
		"frequency_seconds":      req.FrequencySeconds,
		"interval_start":         req.IntervalStart.UTC().Format(time.RFC3339Nano),
		"interval_end":           req.IntervalEnd.UTC().Format(time.RFC3339Nano),
		"producer_offset":        req.ProducerOffset,
		"terminal_result":        string(req.TerminalResult),
		"object_key":             stringOrNil(req.ObjectKey),
		"object_sha256":          stringOrNil(req.ObjectSHA256),
		"source_domain":          req.SourceDomain,
		"strict_reader_eligible": req.StrictReaderEligible,
		"cutoff_at":              req.CutoffAt.UTC().Format(time.RFC3339Nano),
		"predecessor_hash":       stringOrNil(req.PredecessorHash),
		"producer_identity":      producerIdentity,
	}
	buf := &bytes.Buffer{}
	if err := writeCanonical(buf, commit); err != nil {
		return "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

func stringOrNil(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// writeCanonical emits JSON with lexicographic key order at every level.
// Kept in lockstep with the receiver's writeCanonical — if either diverges
// the frontier chain silently fails; test cross-canonicalization when
// changes are made.
func writeCanonical(w *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		w.WriteString("null")
		return nil
	case bool:
		if v {
			w.WriteString("true")
		} else {
			w.WriteString("false")
		}
		return nil
	case int:
		fmt.Fprintf(w, "%d", v)
		return nil
	case int64:
		fmt.Fprintf(w, "%d", v)
		return nil
	case uint64:
		fmt.Fprintf(w, "%d", v)
		return nil
	case float64:
		if v == float64(int64(v)) {
			fmt.Fprintf(w, "%d", int64(v))
			return nil
		}
		return fmt.Errorf("canonical json: non-integer float %g is not supported", v)
	case string:
		encoded, err := json.Marshal(v)
		if err != nil {
			return err
		}
		w.Write(encoded)
		return nil
	case []any:
		w.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				w.WriteByte(',')
			}
			if err := writeCanonical(w, item); err != nil {
				return err
			}
		}
		w.WriteByte(']')
		return nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sortStrings(keys)
		w.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				w.WriteByte(',')
			}
			encoded, err := json.Marshal(k)
			if err != nil {
				return err
			}
			w.Write(encoded)
			w.WriteByte(':')
			if err := writeCanonical(w, v[k]); err != nil {
				return err
			}
		}
		w.WriteByte('}')
		return nil
	default:
		return fmt.Errorf("canonical json: unsupported value type %T", value)
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
