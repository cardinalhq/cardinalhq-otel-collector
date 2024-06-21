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

package chqenforcerprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
)

func TestBoolsToPhase(t *testing.T) {
	tests := []struct {
		name        string
		filtered    bool
		wouldFilter bool
		expected    chqpb.Phase
	}{
		{
			"filtered",
			true,
			false,
			chqpb.Phase_FILTERED,
		},
		{
			"wouldFilter",
			false,
			true,
			chqpb.Phase_DRY_RUN_FILTERED,
		},
		{
			"passthrough",
			false,
			false,
			chqpb.Phase_PASSTHROUGH,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := logBoolsToPhase(tt.filtered, tt.wouldFilter)
			assert.Equal(t, tt.expected, result)
		})
	}
}
