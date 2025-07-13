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
)

func TestServiceNameFromAdddress(t *testing.T) {
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
			got := serviceNameFromAdddress(tt.addr)
			if got != tt.expected {
				t.Errorf("serviceNameFromAdddress(%q) = %q; want %q", tt.addr, got, tt.expected)
			}
		})
	}
}
