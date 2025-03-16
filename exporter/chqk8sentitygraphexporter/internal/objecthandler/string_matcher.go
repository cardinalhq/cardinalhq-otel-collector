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

package objecthandler

import "strings"

type StringMatcher interface {
	Match(string) bool
}

type LiteralMatcher struct {
	s string
}

func NewLiteralMatcher(s string) *LiteralMatcher {
	return &LiteralMatcher{s: s}
}

func (m *LiteralMatcher) Match(s string) bool {
	return m.s == s
}

type PrefixMatcher struct {
	prefix string
}

func NewPrefixMatcher(prefix string) *PrefixMatcher {
	return &PrefixMatcher{prefix: prefix}
}

func (m *PrefixMatcher) Match(s string) bool {
	return strings.HasPrefix(s, m.prefix)
}

type SuffixMatcher struct {
	suffix string
}

func NewSuffixMatcher(suffix string) *SuffixMatcher {
	return &SuffixMatcher{suffix: suffix}
}

func (m *SuffixMatcher) Match(s string) bool {
	return strings.HasSuffix(s, m.suffix)
}
