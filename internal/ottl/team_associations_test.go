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

package ottl

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"testing"
)

func TestTeamAssociations(t *testing.T) {
	statements := []ContextStatement{
		{
			Context: "resource",
			Conditions: []string{
				`IsMatch(attributes["service.name"], "service1|service2|service3")`,
			},
			Statements: []string{
				`set(attributes["team"], "cardinal")`,
			},
		},
	}

	transformations, err := ParseTransformations(statements, zap.NewNop())
	assert.NoError(t, err)
	l := len(transformations.resourceTransforms)
	assert.True(t, l > 0)

	rm1 := pmetric.NewResourceMetrics()
	rm1.Resource().Attributes().PutStr("service.name", "service1")
	transformations.ExecuteResourceMetricTransforms(rm1)

	// check if rm1 attributes have been updated with team = "cardinal"
	team, found := rm1.Resource().Attributes().Get("team")
	assert.True(t, found)
	assert.Equal(t, "cardinal", team.Str())
}
