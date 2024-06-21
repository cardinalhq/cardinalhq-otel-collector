// Copyright 2024 CardinalHQ, Inc
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

package spantagger

import "testing"

func TestError_Error(t *testing.T) {
	tests := []struct {
		name string
		e    Error
		want string
	}{
		{
			"Test InconsistentTraceIDsError",
			InconsistentTraceIDsError,
			"inconsistent traceIDs",
		},
		{
			"Test OrphanedSpanError",
			OrphanedSpanError,
			"orphaned span",
		},
		{
			"Test NoRootError",
			NoRootError,
			"no root span",
		},
		{
			"Test MultipleRootsError",
			MultipleRootsError,
			"multiple root spans",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Error(); got != tt.want {
				t.Errorf("Error.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}
