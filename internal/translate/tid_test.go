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

package translate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestAddKeys(t *testing.T) {
	attr := pcommon.NewMap()
	attr.PutStr("_private", "value1")
	attr.PutStr("key1", "value2")
	attr.PutStr("key2", "value3")

	tags := make(map[string]string)
	addKeys(attr, "prefix", tags)

	expectedTags := map[string]string{
		"prefix.key1": "value2",
		"prefix.key2": "value3",
	}

	assert.Equal(t, expectedTags, tags)
}
