// Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package chqdecoratorprocessor

import (
	"context"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	semconv "go.opentelemetry.io/otel/semconv/v1.22.0"
)

func TestSpansProcessor_FingerprintWithHttpResource(t *testing.T) {
	// Corrected config with pointer fields
	windowSize := 100
	interval := int64(10000)

	// Initialize spansProcessor with the corrected configuration
	config := &Config{
		TracesConfig: TracesConfig{
			EstimatorWindowSize: windowSize, // Use pointer
			EstimatorInterval:   interval,   // Use pointer
		},
	}
	processorSettings := processor.Settings{}
	sp, err := newSpansProcessor(processorSettings, config)
	assert.NoError(t, err)

	// Mock service name
	serviceName := "test-service"

	// Case 1: Span with httpResource
	td := ptrace.NewTraces()
	rss := td.ResourceSpans().AppendEmpty()
	rss.Resource().Attributes().PutStr(string(semconv.ServiceNameKey), serviceName)
	ilss := rss.ScopeSpans().AppendEmpty()
	span := ilss.Spans().AppendEmpty()
	span.Attributes().PutStr(httpMethod, "GET")
	span.Attributes().PutStr(httpRoute, "/users") // httpResource present

	// Process the traces and validate
	_, err = sp.processTraces(context.Background(), td)
	assert.NoError(t, err)

	// Extract the fingerprint for span with httpResource
	fingerprintAttr, exists := span.Attributes().Get(translate.CardinalFieldFingerprint)
	assert.True(t, exists, "Fingerprint should be calculated for span with httpResource")
	assert.NotEqual(t, int64(0), fingerprintAttr.Int(), "Fingerprint should be non-zero for valid span with httpResource")

	// Validate resource was set correctly
	resourceAttr, exists := span.Attributes().Get(translate.CardinalFieldResourceName)
	assert.True(t, exists, "Resource name should be calculated for span with httpResource")
	assert.Equal(t, "GET /users", resourceAttr.Str(), "Resource name should be equal to GET /users")

	// Case 2: Span without httpResource
	tdNoResource := ptrace.NewTraces()
	rssNoResource := tdNoResource.ResourceSpans().AppendEmpty()
	rssNoResource.Resource().Attributes().PutStr(string(semconv.ServiceNameKey), serviceName)
	ilssNoResource := rssNoResource.ScopeSpans().AppendEmpty()
	spanNoResource := ilssNoResource.Spans().AppendEmpty()

	// Set attributes for the span without httpResource
	spanNoResource.Attributes().PutStr("span.kind", "client")
	spanNoResource.Attributes().PutStr(httpMethod, "GET") // No httpResource

	// Process the traces and validate
	_, err = sp.processTraces(context.Background(), tdNoResource)
	assert.NoError(t, err)

	// Extract the fingerprint for the span without httpResource
	fingerprintAttrNoResource, existsNoResource := spanNoResource.Attributes().Get(translate.CardinalFieldFingerprint)
	assert.True(t, existsNoResource, "Fingerprint should be calculated for span without httpResource")
	assert.NotEqual(t, int64(0), fingerprintAttrNoResource.Int(), "Fingerprint should be non-zero for span without httpResource")
}