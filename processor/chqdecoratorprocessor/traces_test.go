package chqdecoratorprocessor

import (
	"context"
	"go.opentelemetry.io/collector/processor"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Helper function to create a spansProcessor with mock transformations
func createSpansProcessorWithTransformations() *spansProcessor {
	// Mock processor settings and config

	settings := processor.Settings{}
	config := &Config{
		TracesConfig: TracesConfig{
			EstimatorWindowSize: 5,
			EstimatorInterval:   1000,
			Transforms: []ContextStatement{
				// Resource level transformations
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
				},
			},
		},
	}

	sp, _ := newSpansProcessor(settings, config)
	return sp
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
	sp := createSpansProcessorWithTransformations()

	// Create the test traces
	td := createTestTraces()

	// Process the traces with the spansProcessor
	processedTraces, err := sp.processTraces(context.Background(), td)
	assert.NoError(t, err)

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
