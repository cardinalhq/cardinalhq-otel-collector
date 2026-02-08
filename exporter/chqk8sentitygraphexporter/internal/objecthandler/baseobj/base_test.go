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

package baseobj

import (
	"testing"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func TestComputeIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		config   *converterconfig.Config
		baseObj  *graphpb.BaseObject
		expected string
	}{
		{
			name: "With namespace",
			config: &converterconfig.Config{
				IDPrefix: "test-prefix",
			},
			baseObj: &graphpb.BaseObject{
				ApiVersion: "v1",
				Kind:       "Pod",
				Namespace:  "default",
				Name:       "test-pod",
			},
			expected: "kubernetes/test-prefix/v1/Pod/default/test-pod",
		},
		{
			name: "Without namespace",
			config: &converterconfig.Config{
				IDPrefix: "test-prefix",
			},
			baseObj: &graphpb.BaseObject{
				ApiVersion: "v1",
				Kind:       "Node",
				Name:       "test-node",
			},
			expected: "kubernetes/test-prefix/v1/Node/test-node",
		},
		{
			name: "Empty IDPrefix",
			config: &converterconfig.Config{
				IDPrefix: "",
			},
			baseObj: &graphpb.BaseObject{
				ApiVersion: "apps/v1",
				Kind:       "Deployment",
				Namespace:  "prod",
				Name:       "test-deployment",
			},
			expected: "kubernetes//apps/v1/Deployment/prod/test-deployment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeIdentifier(tt.config, tt.baseObj)
			if result != tt.expected {
				t.Errorf("computeIdentifier() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func BenchmarkComputeIdentifer(b *testing.B) {
	config := &converterconfig.Config{
		IDPrefix: "test-prefix",
	}
	baseObj := &graphpb.BaseObject{
		ApiVersion: "v1",
		Kind:       "Pod",
		Namespace:  "default",
		Name:       "test-pod",
	}

	for b.Loop() {
		computeIdentifier(config, baseObj)
	}
}
