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

type Config struct {
	HashItems             []string
	IgnoredAnnotations    []StringMatcher
	IgnoredSecretNames    []StringMatcher
	IgnoredConfigMapNames []StringMatcher
}

var (
	defaultIgnoredAnnotations = []StringMatcher{
		NewPrefixMatcher("kubectl.kubernetes.io/"),
		NewPrefixMatcher("internal.config.kubernetes.io/"),
	}
	defaultIgnoredSecretNames = []StringMatcher{
		NewPrefixMatcher("default-token-"),
	}
	defaultIgnoredConfigMapNames = []StringMatcher{
		NewLiteralMatcher("kube-root-ca.crt"),
	}
)

func New() *Config {
	c := &Config{
		HashItems:             []string{},
		IgnoredAnnotations:    defaultIgnoredAnnotations,
		IgnoredSecretNames:    defaultIgnoredSecretNames,
		IgnoredConfigMapNames: defaultIgnoredConfigMapNames,
	}
	return c
}

func (c *Config) WithHashItems(items ...string) *Config {
	c.HashItems = append(c.HashItems, items...)
	return c
}

func (c *Config) WithIgnoredAnnotations(matchers ...StringMatcher) *Config {
	c.IgnoredAnnotations = append(c.IgnoredAnnotations, matchers...)
	return c
}

func (c *Config) WithIgnoredSecretNames(matchers ...StringMatcher) *Config {
	c.IgnoredSecretNames = append(c.IgnoredSecretNames, matchers...)
	return c
}

func (c *Config) WithIgnoredConfigMapNames(matchers ...StringMatcher) *Config {
	c.IgnoredConfigMapNames = append(c.IgnoredConfigMapNames, matchers...)
	return c
}
