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

package pitbullprocessor

import (
	"context"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"testing"
)

// Helper function to convert a CSV-like structure to a LookupTable
func createLookupTable() ottl.LookupTable {
	return ottl.LookupTable{
		{"serviceName": "service1", "team": "team1"},
		{"serviceName": "service2", "team": "team1"},
		{"serviceName": "service3", "team": "team2"},
	}
}

func TestLookupTableBasic(t *testing.T) {
	logger := zap.NewNop()

	lookupTable := createLookupTable()

	// Define a LookupRule that maps "service.name" to "team"
	rule := ottl.LookupRule{
		FieldNamesToSet: []string{"team"},
		Keys: []*ottl.LookupKey{
			{
				ColumnName:     "serviceName",
				OTTLExpression: `resource.attributes["service.name"]`,
			},
		},
	}

	// Create LookupConfig with the rule, LookupTable, and the qualifier
	lookupConfig := ottl.LookupConfig{
		TableName: "test_table",
		Table:     lookupTable,
		LogRules:  []*ottl.LookupRule{&rule},
	}

	lookupConfig.Init(logger)

	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := plog.NewLogRecord()
	rl.Resource().Attributes().PutStr("service.name", "service1")

	transformCtx := ottllog.NewTransformContext(logRecord, sl.Scope(), rl.Resource(), sl, rl)
	lookupConfig.ExecuteLogsRules(context.Background(), transformCtx, logRecord)

	teamAttr, found := logRecord.Attributes().Get("team")
	assert.True(t, found, "Expected 'team' attribute to be found")
	assert.Equal(t, "team1", teamAttr.AsString(), "Expected 'team' attribute to be 'team1'")
}

func TestLookupTableBasicWithErrorInCondition(t *testing.T) {
	logger := zap.NewNop()

	lookupTable := createLookupTable()

	// Define a LookupRule that maps "service.name" to "team"
	rule := ottl.LookupRule{
		FieldNamesToSet: []string{"team"},
		Keys: []*ottl.LookupKey{
			{
				ColumnName:     "serviceName",
				OTTLExpression: `resource.attributes['service.name']`,
			},
		},
	}

	// Create LookupConfig with the rule, LookupTable, and the qualifier
	lookupConfig := ottl.LookupConfig{
		TableName: "test_table",
		Table:     lookupTable,
		LogRules:  []*ottl.LookupRule{&rule},
	}

	lookupConfig.Init(logger)

	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := plog.NewLogRecord()
	rl.Resource().Attributes().PutStr("service.name", "service1")

	transformCtx := ottllog.NewTransformContext(logRecord, sl.Scope(), rl.Resource(), sl, rl)
	lookupConfig.ExecuteLogsRules(context.Background(), transformCtx, logRecord)

	_, found := logRecord.Attributes().Get("team")
	assert.False(t, found, "Expected 'team' attribute to be found")
}

// Helper function to convert a CSV-like structure to a LookupTable
func createLookupTableWithMultipleConditions() ottl.LookupTable {
	return ottl.LookupTable{
		{"serviceName": "service1", "businessUnit": "bu123", "team": "team1"},
		{"serviceName": "service2", "businessUnit": "bu123", "team": "team2"},
		{"serviceName": "service3", "businessUnit": "bu456", "team": "team3"},
	}
}

func TestLookupTableMultipleConditions(t *testing.T) {
	logger := zap.NewNop()

	lookupTable := createLookupTableWithMultipleConditions()

	// Define a LookupRule that maps both "service.name" and "businessUnit" to "team"
	rule := ottl.LookupRule{
		FieldNamesToSet: []string{"team"},
		Keys: []*ottl.LookupKey{
			{
				ColumnName:     "serviceName",
				OTTLExpression: `resource.attributes["service.name"]`,
			},
			{
				ColumnName:     "businessUnit",
				OTTLExpression: `resource.attributes["business.unit"]`,
			},
		},
	}

	lookupConfig := ottl.LookupConfig{
		TableName: "test_table",
		Table:     lookupTable,
		LogRules:  []*ottl.LookupRule{&rule},
	}

	lookupConfig.Init(logger)

	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := plog.NewLogRecord()
	rl.Resource().Attributes().PutStr("service.name", "service1")
	rl.Resource().Attributes().PutStr("business.unit", "bu123")

	transformCtx := ottllog.NewTransformContext(logRecord, sl.Scope(), rl.Resource(), sl, rl)
	lookupConfig.ExecuteLogsRules(context.Background(), transformCtx, logRecord)

	teamAttr, found := logRecord.Attributes().Get("team")
	assert.True(t, found, "Expected 'team' attribute to be found")
	assert.Equal(t, "team1", teamAttr.AsString(), "Expected 'team' attribute to be 'team1'")
}

func TestLookupTableNegativeCondition(t *testing.T) {
	logger := zap.NewNop()

	lookupTable := createLookupTableWithMultipleConditions()

	rule := ottl.LookupRule{
		FieldNamesToSet: []string{"team"},
		Keys: []*ottl.LookupKey{
			{
				ColumnName:     "serviceName",
				OTTLExpression: `resource.attributes["service.name"]`,
			},
			{
				ColumnName:     "businessUnit",
				OTTLExpression: `resource.attributes["business.unit"]`,
			},
		},
	}

	// Create LookupConfig with the rule, LookupTable, and the qualifier
	lookupConfig := ottl.LookupConfig{
		TableName: "test_table",
		Table:     lookupTable,
		LogRules:  []*ottl.LookupRule{&rule},
	}

	lookupConfig.Init(logger)

	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := plog.NewLogRecord()
	rl.Resource().Attributes().PutStr("service.name", "service1")
	// Set business.unit to a value that doesn't exist in the table
	rl.Resource().Attributes().PutStr("business.unit", "bu999")

	transformCtx := ottllog.NewTransformContext(logRecord, sl.Scope(), rl.Resource(), sl, rl)
	lookupConfig.ExecuteLogsRules(context.Background(), transformCtx, logRecord)

	_, found := logRecord.Attributes().Get("team")
	assert.False(t, found, "Expected 'team' attribute NOT to be found since the conditions do not match")
}
