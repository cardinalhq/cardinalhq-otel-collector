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

package converterconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_New(t *testing.T) {
	cfg := New("foo")
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.HashItems)
	assert.ElementsMatch(t, defaultIgnoredAnnotations, cfg.IgnoredAnnotations)
	assert.ElementsMatch(t, defaultIgnoredSecretNames, cfg.IgnoredSecretNames)
	assert.ElementsMatch(t, defaultIgnoredConfigMapNames, cfg.IgnoredConfigMapNames)
}

func TestConfig_WithHashItems(t *testing.T) {
	cfg := New("foo")
	cfg = cfg.WithHashItems("item1", "item2")
	assert.ElementsMatch(t, []string{"item1", "item2"}, cfg.HashItems)
	cfg = cfg.WithHashItems("item3")
	assert.ElementsMatch(t, []string{"item1", "item2", "item3"}, cfg.HashItems)
}

func TestConfig_WithIgnoredAnnotations(t *testing.T) {
	offset := len(defaultIgnoredAnnotations)
	cfg := New("foo")
	matcher := NewPrefixMatcher("example.com/")
	cfg = cfg.WithIgnoredAnnotations(matcher)
	assert.Len(t, cfg.IgnoredAnnotations, offset+1)
	m, ok := cfg.IgnoredAnnotations[offset].(*prefixMatcher)
	require.True(t, ok)
	assert.Equal(t, "example.com/", m.prefix)
}

func TestConfig_WithIgnoredSecretNames(t *testing.T) {
	offset := len(defaultIgnoredSecretNames)
	cfg := New("foo")
	matcher := NewPrefixMatcher("secret-prefix-")
	cfg = cfg.WithIgnoredSecretNames(matcher)
	assert.Len(t, cfg.IgnoredSecretNames, offset+1)
	m, ok := cfg.IgnoredSecretNames[offset].(*prefixMatcher)
	require.True(t, ok)
	assert.Equal(t, "secret-prefix-", m.prefix)
}

func TestConfig_WithIgnoredConfigMapNames(t *testing.T) {
	offset := len(defaultIgnoredConfigMapNames)
	cfg := New("foo")
	matcher := NewPrefixMatcher("configmap-prefix-")
	cfg = cfg.WithIgnoredConfigMapNames(matcher)
	assert.Len(t, cfg.IgnoredConfigMapNames, offset+1)
	m, ok := cfg.IgnoredConfigMapNames[offset].(*prefixMatcher)
	require.True(t, ok)
	assert.Equal(t, "configmap-prefix-", m.prefix)
}
