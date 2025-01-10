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

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestAddAttributes(t *testing.T) {
	m := make(map[string]interface{})
	attrs := pcommon.NewMap()
	attrs.PutStr("_cardinalhq.attribute1", "value1")
	attrs.PutBool("_cardinalhq.attribute2", true)
	attrs.PutStr("attribute2", "value2")
	attrs.PutInt("attribute3", 123)

	addAttributes(m, attrs, "prefix")

	assert.Equal(t, "value1", m["_cardinalhq.attribute1"])
	assert.Equal(t, true, m["_cardinalhq.attribute2"])
	assert.Equal(t, "value2", m["prefix.attribute2"])
	assert.Equal(t, "123", m["prefix.attribute3"])
}
