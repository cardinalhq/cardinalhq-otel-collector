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

package chqdecoratorprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Helper function to create a spansProcessor with mock transformations
func createSpansProcessorWithTransformations(t *testing.T) *chqDecorator {
	// Mock processor settings and config

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	processorSettings := processor.Settings{
		TelemetrySettings: component.TelemetrySettings{Logger: logger},
	}
	config := &Config{
		TracesConfig: TracesConfig{
			EstimatorWindowSize: 5,
			EstimatorInterval:   1000,
		},
	}

	c, err := newCHQDecorator(config, "traces", processorSettings)

	instruction := ottl.Instruction{
		Statements: []ottl.ContextStatement{
			{
				Context:    "resource",
				Conditions: []string{`attributes["service.name"] == "test-service"`},
				Statements: []string{`set(attributes["environment"], "test-env")`},
			},
			// Scope level transformations
			{
				Context:    "scope",
				Conditions: []string{`name == "test-scope"`},
				Statements: []string{`set(attributes["level"], "transformed")`},
			},
			// Span level transformations
			{
				Context:    "span",
				Conditions: []string{`attributes["kind"] == "internal"`},
				Statements: []string{`set(attributes["transformed"], true)`},
			}},
		VendorId: "vendorId",
	}

	c.updateTracesSampling(ottl.SamplerConfig{
		Spans: ottl.EventConfigV1{
			Decorators: []ottl.Instruction{instruction},
		},
	})
	require.NoError(t, err)
	return c
}

// Helper function to create test traces
func createTestTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	attrs := rs.Resource().Attributes()
	attrs.PutStr("service.name", "test-service")

	ils := rs.ScopeSpans().AppendEmpty()
	ils.Scope().SetName("test-scope")

	span := ils.Spans().AppendEmpty()
	span.Attributes().PutStr("kind", "internal")
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(2 * time.Second)))

	return td
}

// TestSpansProcessor_Transformations tests the application of transformations at all levels
func TestSpansProcessor_Transformations(t *testing.T) {
	// Create the spansProcessor with mock transformations
	sp := createSpansProcessorWithTransformations(t)

	// Create the test traces
	td := createTestTraces()

	// Process the traces with the spansProcessor
	processedTraces, err := sp.processTraces(context.Background(), td)
	require.NoError(t, err)

	// Verify the resource-level transformation
	rs := processedTraces.ResourceSpans().At(0)
	environmentAttr, exists := rs.Resource().Attributes().Get("environment")
	assert.True(t, exists)
	assert.Equal(t, "test-env", environmentAttr.Str())

	// Verify the scope-level transformation
	ils := rs.ScopeSpans().At(0)
	scopeAttr, exists := ils.Scope().Attributes().Get("level")
	assert.True(t, exists)
	assert.Equal(t, "transformed", scopeAttr.Str())

	// Verify the span-level transformation
	span := ils.Spans().At(0)
	spanTransformedAttr, exists := span.Attributes().Get("transformed")
	assert.True(t, exists)
	assert.Equal(t, true, spanTransformedAttr.Bool())
}
