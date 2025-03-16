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

import "strings"

// StringMatcher is an interface for matching strings.
// s is the string to run the match against.
type StringMatcher interface {
	Match(s string) bool
}

type literalMatcher struct {
	s string
}

func NewLiteralMatcher(s string) StringMatcher {
	return &literalMatcher{s: s}
}

func (m *literalMatcher) Match(s string) bool {
	return m.s == s
}

type prefixMatcher struct {
	prefix string
}

func NewPrefixMatcher(prefix string) StringMatcher {
	return &prefixMatcher{prefix: prefix}
}

func (m *prefixMatcher) Match(s string) bool {
	return strings.HasPrefix(s, m.prefix)
}

type suffixMatcher struct {
	suffix string
}

func NewSuffixMatcher(suffix string) StringMatcher {
	return &suffixMatcher{suffix: suffix}
}

func (m *suffixMatcher) Match(s string) bool {
	return strings.HasSuffix(s, m.suffix)
}
