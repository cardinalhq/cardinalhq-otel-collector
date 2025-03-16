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

// Config configures the conversion of Kubernetes objects to graph objects.
// This followes a builder pattern, starting with New(), then chaining the
// configuration methods.  The final Config object is usable to create a
// converter.
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

// New creates a new Config object with default values.
//
// By default, no items are hashed, but internally stable fields from
// the object are included ensuring that the hash is not directly
// comparable to another object's hash, or even to another field
// within the same object.
//
// By default, annotations with the prefixes "kubectl.kubernetes.io/" and
// "internal.config.kubernetes.io/" are ignored.
//
// By default, secret names with the prefix "default-token-" are ignored.
//
// By default, configmap names with the literal value "kube-root-ca.crt" are ignored.
func New() *Config {
	c := &Config{
		HashItems:             []string{},
		IgnoredAnnotations:    defaultIgnoredAnnotations,
		IgnoredSecretNames:    defaultIgnoredSecretNames,
		IgnoredConfigMapNames: defaultIgnoredConfigMapNames,
	}
	return c
}

// WithHashItems adds the given items to the list of items to hash when
// calculating the hash of a Kubernetes object.  This helps ensure that
// a hash taken from one object is not the same as the hash taken from
// another object, even if the objects are otherwise identical.  The
// cluster name or ID or other stable but unique identifier should be
// included in this list.
//
// By default, no items are hashed, but internally stable fields from
// the object are included ensuring that the hash is not directly
// comparable to another object's hash, or even to another field
// within the same object.
//
// This hash value is used to idenfity changes inside secrets or configmaps
// without exposing the actual content.
func (c *Config) WithHashItems(items ...string) *Config {
	c.HashItems = append(c.HashItems, items...)
	return c
}

// WithIgnoredAnnotations adds the given matchers to the list of annotations
// to ignore when converting a Kubernetes object to a graph object.
//
// By default, annotations with the prefixes "kubectl.kubernetes.io/" and
// "internal.config.kubernetes.io/" are ignored.
//
// Additional annotations can be ignored by adding matchers here, but
// the default ignored annotations are always included.
func (c *Config) WithIgnoredAnnotations(matchers ...StringMatcher) *Config {
	c.IgnoredAnnotations = append(c.IgnoredAnnotations, matchers...)
	return c
}

// WithIgnoredSecretNames adds the given matchers to the list of secret names
// to ignore when converting a Kubernetes object to a graph object.
// Ignored secret names are not included in the graph object graph when those
// secrets are found directly or indirectly, e.g. through a volume mount.
//
// By default, secret names with the prefix "default-token-" are ignored.
//
// Additional secret names can be ignored by adding matchers here, but
// the default ignored secret names are always included.
func (c *Config) WithIgnoredSecretNames(matchers ...StringMatcher) *Config {
	c.IgnoredSecretNames = append(c.IgnoredSecretNames, matchers...)
	return c
}

// WithIgnoredConfigMapNames adds the given matchers to the list of configmap names
// to ignore when converting a Kubernetes object to a graph object.
// Ignored configmap names are not included in the graph object graph when those
// configmaps are found directly or indirectly, e.g. through a volume mount.
//
// By default, configmap names with the literal value "kube-root-ca.crt" are ignored.
//
// Additional configmap names can be ignored by adding matchers here, but
// the default ignored configmap names are always included.
func (c *Config) WithIgnoredConfigMapNames(matchers ...StringMatcher) *Config {
	c.IgnoredConfigMapNames = append(c.IgnoredConfigMapNames, matchers...)
	return c
}
