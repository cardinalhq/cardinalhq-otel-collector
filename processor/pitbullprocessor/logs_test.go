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

package pitbullprocessor

import (
	"testing"

	"github.com/cardinalhq/oteltools/pkg/translate"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
)

func TestRemoveAllCardinalFields(t *testing.T) {
	attr := pcommon.NewMap()
	attr.PutStr("foo", "bar")
	attr.PutStr(translate.CardinalFieldPrefixDot+"foo.field1", "value1")
	attr.PutStr(translate.CardinalFieldPrefixDot+"field2", "value2")
	attr.PutStr("baz", "qux")

	removeAllCardinalFields(attr)

	assert.Equal(t, 2, attr.Len())

	v, ok := attr.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", v.AsString())

	v, ok = attr.Get("baz")
	assert.True(t, ok)
	assert.Equal(t, "qux", v.AsString())
}

func TestGetServiceName(t *testing.T) {
	attr := pcommon.NewMap()
	attr.PutStr(string(semconv.ServiceNameKey), "my-service")
	serviceName := getServiceName(attr)
	assert.Equal(t, "my-service", serviceName)

	attr = pcommon.NewMap()
	serviceName = getServiceName(attr)
	assert.Equal(t, "unknown", serviceName)
}

func TestGetFingerprint(t *testing.T) {
	attr := pcommon.NewMap()
	attr.PutInt(translate.CardinalFieldFingerprint, 123)
	fingerprint := getFingerprint(attr)
	assert.Equal(t, int64(123), fingerprint)

	attr = pcommon.NewMap()
	fingerprint = getFingerprint(attr)
	assert.Equal(t, int64(0), fingerprint)
}
