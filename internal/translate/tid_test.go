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

func TestCalculateTID(t *testing.T) {
	emptyMap := pcommon.NewMap()

	rattr_key1_value1 := pcommon.NewMap()
	rattr_key1_value1.PutStr("key1", "value1")
	rattr_key2_value1 := pcommon.NewMap()
	rattr_key2_value1.PutStr("key2", "value1")
	rattr_key1_value2 := pcommon.NewMap()
	rattr_key1_value2.PutStr("key1", "value2")
	rattr_key2_value2 := pcommon.NewMap()
	rattr_key2_value2.PutStr("key2", "value2")

	sattr_key1_value1 := pcommon.NewMap()
	sattr_key1_value1.PutStr("key1", "value1")
	sattr_key2_value1 := pcommon.NewMap()
	sattr_key2_value1.PutStr("key2", "value1")
	sattr_key1_value2 := pcommon.NewMap()
	sattr_key1_value2.PutStr("key1", "value2")
	sattr_key2_value2 := pcommon.NewMap()
	sattr_key2_value2.PutStr("key2", "value2")

	iattr_key1_value1 := pcommon.NewMap()
	iattr_key1_value1.PutStr("key1", "value1")
	iattr_key2_value1 := pcommon.NewMap()
	iattr_key2_value1.PutStr("key2", "value1")
	iattr_key1_value2 := pcommon.NewMap()
	iattr_key1_value2.PutStr("key1", "value2")
	iattr_key2_value2 := pcommon.NewMap()
	iattr_key2_value2.PutStr("key2", "value2")

	extra_key1_value1 := map[string]string{"key1": "value1"}
	extra_key2_value1 := map[string]string{"key2": "value1"}
	extra_key1_value2 := map[string]string{"key1": "value2"}
	extra_key2_value2 := map[string]string{"key2": "value2"}

	tests := []struct {
		name     string
		extra    map[string]string
		rattr    pcommon.Map
		sattr    pcommon.Map
		iattr    pcommon.Map
		prefix   string
		expected int64
	}{
		{"empty", map[string]string{}, emptyMap, emptyMap, emptyMap, "", -1205034819632174695},
		{"extra_key1_value1", extra_key1_value1, emptyMap, emptyMap, emptyMap, "", -71784427963547203},
		{"extra_key2_value1", extra_key2_value1, emptyMap, emptyMap, emptyMap, "", 7329503001991617464},
		{"extra_key1_value2", extra_key1_value2, emptyMap, emptyMap, emptyMap, "", -4187388890607458396},
		{"extra_key2_value2", extra_key2_value2, emptyMap, emptyMap, emptyMap, "", -4004932887150847951},

		{"rattr_key1_value1", map[string]string{}, rattr_key1_value1, emptyMap, emptyMap, "", 7713227081626083484},
		{"rattr_key2_value1", map[string]string{}, rattr_key2_value1, emptyMap, emptyMap, "", 3053898447341018405},
		{"rattr_key1_value2", map[string]string{}, rattr_key1_value2, emptyMap, emptyMap, "", -3952887023928665124},
		{"rattr_key2_value2", map[string]string{}, rattr_key2_value2, emptyMap, emptyMap, "", 6076813235896780821},

		{"sattr_key1_value1", map[string]string{}, emptyMap, sattr_key1_value1, emptyMap, "", -2982323596790119742},
		{"sattr_key2_value1", map[string]string{}, emptyMap, sattr_key2_value1, emptyMap, "", 7034642165385384032},
		{"sattr_key1_value2", map[string]string{}, emptyMap, sattr_key1_value2, emptyMap, "", 3805045749113710114},
		{"sattr_key2_value2", map[string]string{}, emptyMap, sattr_key2_value2, emptyMap, "", 312644490737961366},

		{"iattr_key1_value1", map[string]string{}, emptyMap, emptyMap, iattr_key1_value1, "", -4812180440725747178},
		{"iattr_key2_value1", map[string]string{}, emptyMap, emptyMap, iattr_key2_value1, "", 7817598404572385472},
		{"iattr_key1_value2", map[string]string{}, emptyMap, emptyMap, iattr_key1_value2, "", -2535349187564985517},
		{"iattr_key2_value2", map[string]string{}, emptyMap, emptyMap, iattr_key2_value2, "", -1168520017444389875},

		{"iattr_key1_value1", map[string]string{}, emptyMap, emptyMap, iattr_key1_value1, "fx", -2755950955245409346},
		{"iattr_key2_value1", map[string]string{}, emptyMap, emptyMap, iattr_key2_value1, "fx", -1944774599415776463},
		{"iattr_key1_value2", map[string]string{}, emptyMap, emptyMap, iattr_key1_value2, "fx", -3272194252395048902},
		{"iattr_key2_value2", map[string]string{}, emptyMap, emptyMap, iattr_key2_value2, "fx", 7749612084525388665},

		{"everything", extra_key1_value1, rattr_key1_value1, sattr_key1_value1, iattr_key1_value1, "fx", -1157290443396124686},
	}

	tids := make(map[int64]bool)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := CalculateTID(tt.extra, tt.rattr, tt.sattr, tt.iattr, tt.prefix, nil)
			assert.Equal(t, tt.expected, actual)
			assert.False(t, tids[actual], "duplicate TID")
			tids[actual] = true
		})
	}
}
