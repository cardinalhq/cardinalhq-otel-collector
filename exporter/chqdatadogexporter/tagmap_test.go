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

package chqdatadogexporter

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestTagString(t *testing.T) {
	resourceAttrs := pcommon.NewMap()
	scopeAttrs := pcommon.NewMap()
	logAttrs := pcommon.NewMap()

	resourceAttrs.PutStr("container.id", "12345")
	resourceAttrs.PutStr("k8s.persistentvolume.access_mode", "ReadWriteOnce")
	resourceAttrs.PutStr("log.file.path", "/var/log")
	resourceAttrs.PutStr("log.file.name", "app.log")

	scopeAttrs.PutStr("telemetry.sdk.language", "go")

	logAttrs.PutStr("bobs.your", "uncle")
	logAttrs.PutInt("number", 123)
	logAttrs.PutBool("is_true", true)

	expectedTags := "number:123,is_true:true,bobs.your:uncle,language:go,access_mode:ReadWriteOnce,container_id:12345,dirname:/var/log,filename:app.log"
	expectedTagsSplit := strings.Split(expectedTags, ",")
	tagString := tagString(resourceAttrs, scopeAttrs, logAttrs)
	tagStringSplit := strings.Split(tagString, ",")
	assert.ElementsMatch(t, expectedTagsSplit, tagStringSplit)
}

func TestTagStrings(t *testing.T) {
	resourceAttrs := pcommon.NewMap()
	scopeAttrs := pcommon.NewMap()
	logAttrs := pcommon.NewMap()

	resourceAttrs.PutStr("container.id", "12345")
	resourceAttrs.PutStr("k8s.persistentvolume.access_mode", "ReadWriteOnce")
	resourceAttrs.PutStr("log.file.path", "/var/log")
	resourceAttrs.PutStr("log.file.name", "app.log")
	resourceAttrs.PutStr("bob", "uncle")

	scopeAttrs.PutStr("telemetry.sdk.language", "go")
	scopeAttrs.PutStr("bobscope", "unclescope")

	logAttrs.PutStr("bobs.your", "uncle")
	logAttrs.PutInt("number", 123)
	logAttrs.PutBool("is_true", true)

	logAttrs.PutStr("_cardinalhq.ignoreme", "ignoreme")

	expectedTags := []string{
		"number:123",
		"is_true:true",
		"bobs.your:uncle",
		"language:go",
		"bobscope:unclescope",
	}
	expectedResources := []string{
		"container_id:12345",
		"access_mode:ReadWriteOnce",
		"dirname:/var/log",
		"filename:app.log",
		"bob:uncle",
	}

	tags, resources := tagStrings(resourceAttrs, scopeAttrs, logAttrs)
	assert.ElementsMatch(t, expectedTags, tags)
	assert.ElementsMatch(t, expectedResources, resources)
}
