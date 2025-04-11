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

package fingerprintprocessor

import (
	"os"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpanFingerprintWithExceptions(t *testing.T) {
	exemplarData1, err := os.ReadFile("testdata/span_error_1.json")
	assert.NoError(t, err, "Failed to read exemplar data")
	exemplarData2, err := os.ReadFile("testdata/span_error_2.json")
	assert.NoError(t, err, "Failed to read exemplar data")

	unmarshaller := ptrace.JSONUnmarshaler{}
	exemplar1, err := unmarshaller.UnmarshalTraces(exemplarData1)
	require.NoError(t, err)
	assert.NotNil(t, exemplar1)
	exemplar2, err := unmarshaller.UnmarshalTraces(exemplarData2)
	require.NoError(t, err)
	assert.NotNil(t, exemplar2)

	e := &fingerprintProcessor{
		traceFingerprinter: fingerprinter.NewFingerprinter(fingerprinter.WithMaxTokens(30)),
	}
	span1 := exemplar1.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	fp1 := e.calculateSpanFingerprint(span1)
	span2 := exemplar2.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	fp2 := e.calculateSpanFingerprint(span2)

	assert.Equal(t, fp1, fp2)
}
