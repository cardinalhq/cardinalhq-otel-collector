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

package chqspannerprocessor

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestServiceNameFromAddress(t *testing.T) {
	tests := []struct {
		addr     string
		expected string
	}{
		{"", ""},
		{"localhost", ""},
		{"127.0.0.1", ""},
		{"prod-us-east-2-global.cluster-asdlasdlj.us-east-2.rds.amazonaws.com", "prod-us-east-2-global"},
		{"prod-us-east-2-global-1.cluster-asdlasdlj.us-east-2.rds.amazonaws.com", "prod-us-east-2-global-1"},
		{"prod-us-east-2-global-1.asdlasdlj.us-east-2.rds.amazonaws.com", "prod-us-east-2-global"},
		{"not-an-rds-address.com", ""},
		{"prod-us-east-2-global.cluster-asdlasdlj.us-east-2.rds.amazonaws.com.extra", ""},
		{"prod-us-east-2-global.rds.amazonaws.com", ""},
		{"prod-us-east-2-global.cluster-asdlasdlj.us-east-2.rds.amazonaws.com", "prod-us-east-2-global"},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			got := serviceNameFromAddress(tt.addr)
			if got != tt.expected {
				t.Errorf("serviceNameFromAdddress(%q) = %q; want %q", tt.addr, got, tt.expected)
			}
		})
	}
}

func TestServiceNameFromServerAddress(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		expected string
	}{
		{"empty address", "", ""},
		{"localhost", "localhost", ""},
		{"127.0.0.1", "127.0.0.1", ""},
		{"valid cluster", "prod-us-east-2-global.cluster-asdlasdlj.us-east-2.rds.amazonaws.com", "prod-us-east-2-global"},
		{"valid cluster with dash", "prod-us-east-2-global-1.cluster-asdlasdlj.us-east-2.rds.amazonaws.com", "prod-us-east-2-global-1"},
		{"valid non-cluster", "prod-us-east-2-global-1.asdlasdlj.us-east-2.rds.amazonaws.com", "prod-us-east-2-global"},
		{"not rds", "not-an-rds-address.com", ""},
		{"extra suffix", "prod-us-east-2-global.cluster-asdlasdlj.us-east-2.rds.amazonaws.com.extra", ""},
		{"short rds", "prod-us-east-2-global.rds.amazonaws.com", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			span := ptrace.NewSpan()
			attrs := pcommon.NewMap()
			attrs.PutStr("server.address", tt.addr)
			span.Attributes().Clear()
			attrs.CopyTo(span.Attributes())
			got := serviceNameFromServerAddress(span)
			if got != tt.expected {
				t.Errorf("serviceNameFromServerAddress(%q) = %q; want %q", tt.addr, got, tt.expected)
			}
		})
	}

	t.Run("missing server.address attribute", func(t *testing.T) {
		span := ptrace.NewSpan()
		span.Attributes().Clear()
		got := serviceNameFromServerAddress(span)
		if got != "" {
			t.Errorf("serviceNameFromServerAddress with missing attribute = %q; want \"\"", got)
		}
	})
}
