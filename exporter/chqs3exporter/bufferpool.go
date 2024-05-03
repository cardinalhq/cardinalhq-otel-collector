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

package chqs3exporter

import (
	"bytes"
	"sync"
)

// This is shared across all instances of the exporter.

var bufferpool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func getBuffer() *bytes.Buffer {
	return bufferpool.Get().(*bytes.Buffer)
}

func putBuffer(b *bytes.Buffer) {
	b.Reset()
	bufferpool.Put(b)
}
