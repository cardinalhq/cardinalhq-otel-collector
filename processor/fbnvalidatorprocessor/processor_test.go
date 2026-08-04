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
	"context"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/fbnvalidatorprocessor/internal/metadata"
)

// The worked example from the freenet spec: 32-byte pubkey; the fingerprint
// (the id UIs display) is base58 of its first 12 bytes.
const (
	examplePubKey = "76nQMZw2XGS84uxcorLWDyxotcfgBKPYsVkJGmy4iMwd"
	exampleNodeID = "2iCMdyUHBasTf8u7o"
)

type fakeAuth map[string]any

func (f fakeAuth) GetAttribute(name string) any { return f[name] }
func (f fakeAuth) GetAttributeNames() []string {
	names := make([]string, 0, len(f))
	for k := range f {
		names = append(names, k)
	}
	return names
}

func authCtx(auth client.AuthData) context.Context {
	return client.NewContext(context.Background(), client.Info{Auth: auth})
}

func newTestValidator(t *testing.T) *validator {
	cfg := &Config{
		Prefixes: map[string]PrefixConfig{
			"freenet": {ValidationAlgorithm: ValidationAlgoFreenet},
		},
	}
	require.NoError(t, cfg.Validate())
	v, err := newValidator(cfg, processortest.NewNopSettings(processortest.NopType))
	require.NoError(t, err)
	return v
}

// resourceAttrs is one resource's identity attributes; nil values are omitted.
type resourceAttrs struct {
	pubkey      string
	fingerprint string
}

func logsWith(resources ...resourceAttrs) plog.Logs {
	ld := plog.NewLogs()
	for _, ra := range resources {
		rl := ld.ResourceLogs().AppendEmpty()
		if ra.pubkey != "" {
			rl.Resource().Attributes().PutStr(defaultPublicKeyAttribute, ra.pubkey)
		}
		if ra.fingerprint != "" {
			rl.Resource().Attributes().PutStr(defaultFingerprintAttribute, ra.fingerprint)
		}
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	}
	return ld
}

func TestExampleDerivation(t *testing.T) {
	pub, err := base58.Decode(examplePubKey)
	require.NoError(t, err)
	require.Len(t, pub, 32)
	assert.Equal(t, exampleNodeID, base58.Encode(pub[:nodeIDBytes]))
}

// Unset attribute names resolve to the defaults at the point of use, so a
// config that was never mutated by Validate still matches real payloads.
func TestDefaultsAttributeNames(t *testing.T) {
	v := newTestValidator(t)
	assert.Empty(t, v.config.Prefixes["freenet"].PublicKeyAttribute, "Validate must not mutate config")

	id, deny := v.expectedIdentity(
		authCtx(fakeAuth{attrPrefix: "freenet", attrPublicKey: examplePubKey}))
	require.Empty(t, deny)
	assert.Equal(t, defaultPublicKeyAttribute, id.pubkeyAttr)
	assert.Equal(t, defaultFingerprintAttribute, id.fingerprintAttr)
}

func TestCustomAttributeNames(t *testing.T) {
	cfg := &Config{
		Prefixes: map[string]PrefixConfig{
			"freenet": {
				ValidationAlgorithm:  ValidationAlgoFreenet,
				PublicKeyAttribute:   "my.pubkey",
				FingerprintAttribute: "my.fp",
			},
		},
	}
	require.NoError(t, cfg.Validate())
	v, err := newValidator(cfg, processortest.NewNopSettings(processortest.NopType))
	require.NoError(t, err)

	ctx := authCtx(fakeAuth{attrPrefix: "freenet", attrPublicKey: examplePubKey})
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("my.pubkey", examplePubKey)
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")

	out, err := v.processLogs(ctx, ld)
	require.NoError(t, err)
	assert.Equal(t, 1, out.ResourceLogs().Len())

	// The default name is not consulted when an override is configured.
	assert.Error(t, mustErr(v.processLogs(ctx, logsWith(resourceAttrs{pubkey: examplePubKey}))))
}

func mustErr(_ plog.Logs, err error) error { return err }

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"no prefixes", Config{}, false},
		{"unsupported algorithm", Config{Prefixes: map[string]PrefixConfig{
			"freenet": {ValidationAlgorithm: "nope"}}}, false},
		{"empty algorithm", Config{Prefixes: map[string]PrefixConfig{
			"freenet": {}}}, false},
		{"valid", Config{Prefixes: map[string]PrefixConfig{
			"freenet": {ValidationAlgorithm: ValidationAlgoFreenet}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestProcessLogs_KeepsMatchingDropsMismatched(t *testing.T) {
	v := newTestValidator(t)
	ctx := authCtx(fakeAuth{attrPrefix: "freenet", attrPublicKey: examplePubKey})
	ld := logsWith(
		resourceAttrs{pubkey: examplePubKey},                              // keep: pubkey matches
		resourceAttrs{fingerprint: exampleNodeID},                         // keep: fingerprint matches
		resourceAttrs{pubkey: examplePubKey, fingerprint: exampleNodeID},  // keep: both match
		resourceAttrs{pubkey: "someone-else"},                             // drop: wrong pubkey
		resourceAttrs{fingerprint: "someone-else"},                        // drop: wrong fingerprint
		resourceAttrs{pubkey: examplePubKey, fingerprint: "someone-else"}, // drop: any mismatch kills it
		resourceAttrs{}, // drop: no identity at all
	)

	out, err := v.processLogs(ctx, ld)
	require.NoError(t, err)
	assert.Equal(t, 3, out.ResourceLogs().Len())
}

// A batch whose resources all fail validation is refused, not silently
// swallowed: the sender must not be told its data was accepted.
func TestProcessLogs_AllMismatchedRejected(t *testing.T) {
	v := newTestValidator(t)
	ctx := authCtx(fakeAuth{attrPrefix: "freenet", attrPublicKey: examplePubKey})

	_, err := v.processLogs(ctx, logsWith(
		resourceAttrs{pubkey: "wrong1"}, resourceAttrs{fingerprint: "wrong2"}))
	assert.ErrorIs(t, err, errRejected)
	assert.True(t, consumererror.IsPermanent(err), "must not be retried")
}

// An empty batch carries nothing to validate and is not an auth failure.
func TestProcessLogs_EmptyBatchSkipped(t *testing.T) {
	v := newTestValidator(t)
	_, err := v.processLogs(context.Background(), plog.NewLogs())
	assert.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
}

func TestProcessLogs_FailClosed(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{"no auth context", context.Background()},
		{"unknown prefix", authCtx(fakeAuth{attrPrefix: "other", attrPublicKey: examplePubKey})},
		{"missing pubkey", authCtx(fakeAuth{attrPrefix: "freenet"})},
		{"garbage pubkey", authCtx(fakeAuth{attrPrefix: "freenet", attrPublicKey: "!!!"})},
		{"pubkey too short", authCtx(fakeAuth{attrPrefix: "freenet", attrPublicKey: "2iCM"})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newTestValidator(t)
			_, err := v.processLogs(tt.ctx, logsWith(resourceAttrs{pubkey: examplePubKey}))
			assert.ErrorIs(t, err, errRejected)
			assert.True(t, consumererror.IsPermanent(err), "must not be retried")
		})
	}
}

// processMetrics and processTraces mirror processLogs; these pin that the
// keep/drop and fail-closed behavior is actually wired up on all three
// signals, not just the one with detailed coverage.
func TestProcessMetricsAndTraces(t *testing.T) {
	goodCtx := authCtx(fakeAuth{attrPrefix: "freenet", attrPublicKey: examplePubKey})

	t.Run("metrics keep and drop", func(t *testing.T) {
		v := newTestValidator(t)
		md := pmetric.NewMetrics()
		for _, pk := range []string{examplePubKey, "someone-else"} {
			rm := md.ResourceMetrics().AppendEmpty()
			rm.Resource().Attributes().PutStr(defaultPublicKeyAttribute, pk)
			rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("m")
		}
		out, err := v.processMetrics(goodCtx, md)
		require.NoError(t, err)
		assert.Equal(t, 1, out.ResourceMetrics().Len())
		assert.Equal(t, examplePubKey,
			mustAttr(t, out.ResourceMetrics().At(0).Resource(), defaultPublicKeyAttribute))
	})

	t.Run("metrics fail closed", func(t *testing.T) {
		v := newTestValidator(t)
		md := pmetric.NewMetrics()
		md.ResourceMetrics().AppendEmpty().Resource().
			Attributes().PutStr(defaultPublicKeyAttribute, examplePubKey)
		_, err := v.processMetrics(context.Background(), md)
		assert.ErrorIs(t, err, errRejected)
	})

	t.Run("traces keep and drop", func(t *testing.T) {
		v := newTestValidator(t)
		td := ptrace.NewTraces()
		for _, pk := range []string{examplePubKey, "someone-else"} {
			rs := td.ResourceSpans().AppendEmpty()
			rs.Resource().Attributes().PutStr(defaultPublicKeyAttribute, pk)
			rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("s")
		}
		out, err := v.processTraces(goodCtx, td)
		require.NoError(t, err)
		assert.Equal(t, 1, out.ResourceSpans().Len())
		assert.Equal(t, examplePubKey,
			mustAttr(t, out.ResourceSpans().At(0).Resource(), defaultPublicKeyAttribute))
	})

	t.Run("traces fail closed", func(t *testing.T) {
		v := newTestValidator(t)
		td := ptrace.NewTraces()
		td.ResourceSpans().AppendEmpty().Resource().
			Attributes().PutStr(defaultPublicKeyAttribute, examplePubKey)
		_, err := v.processTraces(context.Background(), td)
		assert.ErrorIs(t, err, errRejected)
	})
}

func mustAttr(t *testing.T, res pcommon.Resource, name string) string {
	t.Helper()
	val, ok := res.Attributes().Get(name)
	require.True(t, ok)
	return val.AsString()
}

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	require.NotNil(t, cfg)

	c := cfg.(*Config)
	c.Prefixes = map[string]PrefixConfig{
		"freenet": {ValidationAlgorithm: ValidationAlgoFreenet},
	}
	require.NoError(t, c.Validate())

	p, err := f.CreateLogs(context.Background(),
		processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	assert.True(t, p.Capabilities().MutatesData)
}
