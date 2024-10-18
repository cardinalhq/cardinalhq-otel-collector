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

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func createTestResourceLogs() plog.ResourceLogs {
	rl := plog.NewResourceLogs()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	return rl
}

func createTestScopeLogs() plog.ScopeLogs {
	sl := plog.NewScopeLogs()
	sl.Scope().SetName("test-scope")
	return sl
}

func createTestLogRecord() plog.LogRecord {
	ll := plog.NewLogRecord()
	ll.Attributes().PutStr("log.level", "INFO")
	return ll
}

// TestFilterRule_ResourceConditionLog Test resource-based condition for logs
func TestFilterRule_ResourceConditionLog(t *testing.T) {
	// Create a filterRule with a resource-based condition
	instruction := Instruction{
		VendorId: "datadog",
		Statements: []ContextStatement{
			{
				Context: "log",
				RuleId:  "test-rule",
				Conditions: []string{
					`resource.attributes["service.name"] == "test-service"`,
				},
				Statements: []string{
					`set(attributes["dropped"], true)`,
				},
			},
		},
	}
	transformations, err := ParseTransformations(instruction, zap.NewNop())
	require.NoError(t, err)

	// Create test data
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()
	ll := createTestLogRecord()

	transformCtx := ottllog.NewTransformContext(ll, sl.Scope(), rl.Resource(), sl, rl)

	transformations.ExecuteLogTransforms(nil, transformCtx, "", pcommon.NewSlice())

	dropped, exists := ll.Attributes().Get("dropped")
	require.True(t, exists)
	require.True(t, dropped.Bool())
}
