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

package chqexemplarprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/cardinalhq/oteltools/pkg/translate"
)

func TestGetFingerprint(t *testing.T) {
	attr := pcommon.NewMap()
	attr.PutInt(translate.CardinalFieldFingerprint, 123)
	fingerprint := getFingerprint(attr)
	assert.Equal(t, int64(123), fingerprint)

	attr = pcommon.NewMap()
	fingerprint = getFingerprint(attr)
	assert.Equal(t, int64(0), fingerprint)
}
