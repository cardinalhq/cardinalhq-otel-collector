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
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
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

	instruction := Instruction{
		Statements: statements,
	}
	transformations, err := ParseTransformations(instruction, zap.NewNop())
	assert.NoError(t, err)
	l := len(transformations.resourceTransformsByRuleId)
	assert.True(t, l > 0)

	rm1 := pmetric.NewResourceMetrics()
	rm1.Resource().Attributes().PutStr("service.name", "service1")
	tc := ottlresource.NewTransformContext(rm1.Resource(), rm1)
	transformations.ExecuteResourceTransforms(nil, tc, "vendorId", pcommon.NewSlice())

	// check if rm1 attributes have been updated with team = "cardinal"
	team, found := rm1.Resource().Attributes().Get("team")
	assert.True(t, found)
	assert.Equal(t, "cardinal", team.Str())
}
