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

package boxer

import (
	"go.opentelemetry.io/collector/component"
)

func BoxerFor(path string, kind component.Kind, ent component.ID, name string, boxerOpts ...BoxerOptions) (*Boxer, error) {
	var storage Buffer
	if path == "" {
		storage = NewMemoryBuffer()
	} else {
		storage = NewFilesystemBuffer(path)
	}
	box, err := NewBoxer(append(boxerOpts, WithBufferStorage(storage))...)
	if err != nil {
		_ = storage.Shutdown()
		return nil, err
	}
	return box, nil
}
