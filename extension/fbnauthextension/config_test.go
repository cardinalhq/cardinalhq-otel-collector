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
