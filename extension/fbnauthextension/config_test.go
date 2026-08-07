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

package fbnauthextension

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/fbnauthextension/internal/metadata"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	// The skew default lives here, not in Validate: a config that skipped
	// validation must not end up with a zero window that denies everything.
	assert.Equal(t, defaultMaxClockSkew, cfg.MaxClockSkew)
}

func TestConfigValidate(t *testing.T) {
	valid := map[string]PrefixConfig{"freenet": {SignatureAlgo: SignatureAlgoXEd25519}}

	tests := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"no prefixes", Config{MaxClockSkew: time.Minute}, false},
		{"empty algo", Config{MaxClockSkew: time.Minute,
			Prefixes: map[string]PrefixConfig{"freenet": {}}}, false},
		{"unsupported algo", Config{MaxClockSkew: time.Minute,
			Prefixes: map[string]PrefixConfig{"freenet": {SignatureAlgo: "rsa"}}}, false},
		// Zero is rejected rather than silently replaced with the default, so
		// "max_clock_skew: 0" cannot read as "disable the skew check".
		{"zero skew", Config{Prefixes: valid}, false},
		{"negative skew", Config{MaxClockSkew: -time.Minute, Prefixes: valid}, false},
		{"valid ed25519", Config{MaxClockSkew: time.Minute,
			Prefixes: map[string]PrefixConfig{"freenet": {SignatureAlgo: SignatureAlgoEd25519}}}, true},
		{"valid xed25519", Config{MaxClockSkew: time.Minute, Prefixes: valid}, true},
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

// The canonical-URL table from the freenet side (its
// the_audience_hash_is_reproducible_from_the_documented_canonical_url test):
// each URL a node might be pointed at must hash as the canonical URL it
// reduces to. If these two sides disagree, every token is wrong_audience.
func TestAudienceHashCanonicalization(t *testing.T) {
	tests := []struct{ requestURL, canonical string }{
		{"http://collector.example:4318/v1/metrics", "http://collector.example:4318/v1/metrics"},
		{"https://Collector.Example/v1/metrics", "https://collector.example:443/v1/metrics"},
		{"https://user:secret@collector.example:4318/v1/metrics", "https://collector.example:4318/v1/metrics"},
		{"http://user:pass@host:1234/path/here", "http://host:1234/path/here"},
	}
	for _, tt := range tests {
		got, err := AudienceHash(tt.requestURL)
		require.NoError(t, err)
		want, err := AudienceHash(tt.canonical)
		require.NoError(t, err)
		assert.Equal(t, want, got, tt.requestURL)
	}
}

// Golden values computed outside this package (sha256 of the canonical string,
// first 16 bytes, base58). Pins the formula itself, not just self-consistency —
// the canonicalization above would pass even if we hashed the wrong string.
// The canonical strings behind these, which are what the freenet side hashes:
// "collector.example:4318/v1/metrics" and "collector.example:443/v1/metrics".
func TestAudienceHashGolden(t *testing.T) {
	for url, want := range map[string]string{
		"http://collector.example:4318/v1/metrics": "QeBzi6joRxb8ZXD1scRqoq",
		"https://collector.example/v1/metrics":     "J72yJMQ8Kvzno9a7DBDz6b",
	} {
		got, err := AudienceHash(url)
		require.NoError(t, err)
		assert.Equal(t, want, got, url)
	}
}

func TestAudienceHashDistinctions(t *testing.T) {
	base, err := AudienceHash("https://c.example/v1/metrics")
	require.NoError(t, err)

	// Query and fragment are dropped; an OTLP export URL has neither. The
	// scheme is deliberately not bound either — one collector reachable both
	// ways must not need two audience entries — so pin that here rather than
	// leave it to be "fixed" back, which is exactly how this side and the
	// freenet side drifted apart once.
	for _, same := range []string{
		"https://c.example/v1/metrics?x=1",
		"https://c.example/v1/metrics#f",
		"https://c.example:443/v1/metrics",
		"http://c.example:443/v1/metrics",
	} {
		h, err := AudienceHash(same)
		require.NoError(t, err)
		assert.Equal(t, base, h, same)
	}

	// Path is verbatim: a trailing slash or a dot segment is a different URL,
	// because it is a different string on the sender's side too.
	for _, differs := range []string{
		"https://c.example/v1/metrics/",
		"https://c.example/v2/../v1/metrics",
		"http://c.example/v1/metrics",
		"https://c.example:4318/v1/metrics",
	} {
		h, err := AudienceHash(differs)
		require.NoError(t, err)
		assert.NotEqual(t, base, h, differs)
	}
}

func TestAudienceHashRejects(t *testing.T) {
	// Fails closed rather than guessing a port or a host: a typo in the
	// audience list must surface at startup, not as blanket denials later.
	for _, bad := range []string{"", "collector.example/v1/metrics", "ftp://c.example/x", "https:///v1/metrics"} {
		_, err := AudienceHash(bad)
		assert.Error(t, err, bad)
	}
}

func TestConfigValidateRejectsBadAudience(t *testing.T) {
	cfg := Config{MaxClockSkew: time.Minute, Prefixes: map[string]PrefixConfig{
		"freenet": {SignatureAlgo: SignatureAlgoXEd25519, Audiences: []string{"not-a-url"}},
	}}
	assert.Error(t, cfg.Validate())
}

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Prefixes = map[string]PrefixConfig{"freenet": {SignatureAlgo: SignatureAlgoXEd25519}}
	require.NoError(t, cfg.Validate())

	ext, err := f.Create(context.Background(),
		extensiontest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NotNil(t, ext)
}
