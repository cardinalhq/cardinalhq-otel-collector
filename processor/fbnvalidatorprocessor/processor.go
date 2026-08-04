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

package fbnvalidatorprocessor

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/fbnvalidatorprocessor/internal/metadata"
)

// Auth attribute names published by the fbn_auth extension; keep in sync.
const (
	attrPrefix    = "prefix"
	attrPublicKey = "public_key"
)

// nodeIDBytes is how many leading bytes of the decoded public key form the
// node id under the "freenet" validation algorithm.
const nodeIDBytes = 12

var processorCapabilities = consumer.Capabilities{MutatesData: true}

type validator struct {
	config           *Config
	logger           *zap.Logger
	resourcesDropped metric.Int64Counter
}

func newValidator(cfg *Config, set processor.Settings) (*validator, error) {
	m, err := metadata.Meter(set.TelemetrySettings).Int64Counter("resources_dropped")
	if err != nil {
		return nil, err
	}
	return &validator{
		config:           cfg,
		logger:           set.Logger,
		resourcesDropped: m,
	}, nil
}

// identity is what the authenticated transport wrapper says about the sender:
// its full base58 public key and the fingerprint derived from it, plus which
// resource attributes payloads carry them in.
type identity struct {
	pubkey          string // full base58 public key, as authenticated
	fingerprint     string // base58(first nodeIDBytes of the decoded key)
	pubkeyAttr      string
	fingerprintAttr string
}

// expectedIdentity derives the identity payloads must carry, from the
// authenticated context. It returns (nil, reason) when the batch must be
// dropped entirely (fail closed): no auth context, a prefix we have no
// validation rule for, or an undecodable public key. Both expected values
// come from the verified auth token — never from the payload — so a sender
// cannot claim another node's key or fingerprint.
func (v *validator) expectedIdentity(ctx context.Context) (*identity, string) {
	auth := client.FromContext(ctx).Auth
	if auth == nil {
		return nil, "no_auth_context"
	}
	prefix, _ := auth.GetAttribute(attrPrefix).(string)
	pc, ok := v.config.Prefixes[prefix]
	if !ok {
		return nil, "unknown_prefix"
	}
	pubkeyB58, _ := auth.GetAttribute(attrPublicKey).(string)
	pubkey, err := base58.Decode(pubkeyB58)
	if err != nil || len(pubkey) < nodeIDBytes {
		return nil, "bad_pubkey"
	}
	// ValidationAlgorithm is guaranteed "freenet" by Config.Validate.
	// Attribute names are defaulted here rather than in Config.Validate so an
	// unvalidated config cannot silently produce empty names that match
	// nothing and drop every resource.
	return &identity{
		pubkey:          pubkeyB58,
		fingerprint:     base58.Encode(pubkey[:nodeIDBytes]),
		pubkeyAttr:      cmp.Or(pc.PublicKeyAttribute, defaultPublicKeyAttribute),
		fingerprintAttr: cmp.Or(pc.FingerprintAttribute, defaultFingerprintAttribute),
	}, ""
}

// keep reports whether a resource matches the authenticated identity: every
// identity attribute it carries must match, and it must carry at least one.
// Mismatches and identity-less resources are dropped (counted separately).
func (v *validator) keep(ctx context.Context, res pcommon.Resource, id *identity) bool {
	matched := false
	if val, ok := res.Attributes().Get(id.pubkeyAttr); ok {
		if val.AsString() != id.pubkey {
			v.drop(ctx, "pubkey_mismatch", id.pubkeyAttr, val.AsString(), id.pubkey)
			return false
		}
		matched = true
	}
	if val, ok := res.Attributes().Get(id.fingerprintAttr); ok {
		if val.AsString() != id.fingerprint {
			v.drop(ctx, "fingerprint_mismatch", id.fingerprintAttr, val.AsString(), id.fingerprint)
			return false
		}
		matched = true
	}
	if !matched {
		v.drop(ctx, "no_identity_attribute", id.pubkeyAttr, "", id.pubkey)
	}
	return matched
}

// drop counts a dropped resource and logs what it claimed versus what the
// authenticated sender is, so "where did node X's telemetry go?" is
// answerable from logs and not just a reason-labelled counter.
func (v *validator) drop(ctx context.Context, reason, attr, got, want string) {
	v.count(ctx, reason)
	v.logger.Debug("fbn_validator dropping resource",
		zap.String("reason", reason),
		zap.String("attribute", attr),
		zap.String("got", got),
		zap.String("authenticated", want))
}

func (v *validator) count(ctx context.Context, reason string) {
	v.resourcesDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// errRejected is returned to the sender when a batch is refused outright.
// It is always wrapped as a permanent consumer error: nothing about a failed
// identity check improves on retry, so the data must not be requeued.
var errRejected = errors.New("fbn_validator rejected batch")

func rejected(reason string) error {
	return consumererror.NewPermanent(fmt.Errorf("%w: %s", errRejected, reason))
}

// batchIdentity resolves the identity a batch of n resources must match.
// A non-nil error is the caller's return value: the batch is refused (fail
// closed) and the sender is told why, rather than being handed a success it
// would take for delivery.
func (v *validator) batchIdentity(ctx context.Context, n int) (*identity, error) {
	if n == 0 {
		// Nothing to validate; not an auth failure.
		return nil, processorhelper.ErrSkipProcessingData
	}
	id, deny := v.expectedIdentity(ctx)
	if deny != "" {
		for range n {
			v.count(ctx, deny)
		}
		v.logger.Warn("fbn_validator rejecting batch",
			zap.String("reason", deny), zap.Int("resources", n))
		return nil, rejected(deny)
	}
	return id, nil
}

// batchResult reports the outcome once per-resource filtering has run.
// remaining == 0 means every resource failed validation (each already counted
// by keep), so the whole batch is refused.
func (v *validator) batchResult(remaining int) error {
	if remaining == 0 {
		return rejected("no_matching_resources")
	}
	return nil
}

func (v *validator) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	id, err := v.batchIdentity(ctx, ld.ResourceLogs().Len())
	if err != nil {
		return ld, err
	}
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		return !v.keep(ctx, rl.Resource(), id)
	})
	return ld, v.batchResult(ld.ResourceLogs().Len())
}

func (v *validator) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	id, err := v.batchIdentity(ctx, md.ResourceMetrics().Len())
	if err != nil {
		return md, err
	}
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		return !v.keep(ctx, rm.Resource(), id)
	})
	return md, v.batchResult(md.ResourceMetrics().Len())
}

func (v *validator) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	id, err := v.batchIdentity(ctx, td.ResourceSpans().Len())
	if err != nil {
		return td, err
	}
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		return !v.keep(ctx, rs.Resource(), id)
	})
	return td, v.batchResult(td.ResourceSpans().Len())
}
