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
	"testing"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func TestGetServiceName(t *testing.T) {
	attr := pcommon.NewMap()
	attr.PutStr(string(semconv.ServiceNameKey), "my-service")
	serviceName := getServiceName(attr)
	assert.Equal(t, "my-service", serviceName)

	attr = pcommon.NewMap()
	serviceName = getServiceName(attr)
	assert.Equal(t, "unknown", serviceName)
}

func TestTokenFields(t *testing.T) {
	e := &fingerprintProcessor{
		logFingerprinter: fingerprinter.NewFingerprinter(fingerprinter.WithMaxTokens(30)),
		tenants:          make(map[string]*tenantState),
	}
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("INFO [2025-01-12T05:28:19.575Z] \"POST /tickets HTTP/1.1\" 201 - via_upstream - \"-\" REDACTED 8737 \"54.162.8.237,172.25.31.44\" \"Ruby\" \"7feb561e-2095-483f-b1c5-0c95c8eb7ddb\" \"aiops-test10.freshstatus-sta91ng.io\" \"172.25.26.133:8181\" outbound|80|BLUE|aiops-tickets.ams-aiops-tickets-staging.svc.cluster.local 172.25.27.204:45834 172.25.27.204:8080 172.25.31.44:46526 - -\n")
	_, level, err := e.addTokenFields("default", lr)
	assert.NoError(t, err)
	assert.Equal(t, "info", level)
	tMap, found := lr.Attributes().Get(translate.CardinalFieldTokenMap)
	assert.True(t, found)
	tokenMap := tMap.Map()
	assert.NotNil(t, tokenMap)
	num0, num0Found := tokenMap.Get("<Number>_0")
	assert.True(t, num0Found)
	assert.Equal(t, "201", num0.Str())
	num1, num1Found := tokenMap.Get("<Number>_1")
	assert.True(t, num1Found)
	assert.Equal(t, "8737", num1.Str())
}
