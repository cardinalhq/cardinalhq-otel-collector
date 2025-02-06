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
	"sync/atomic"
)

type MapStore struct {
	data atomic.Value
}

func NewMapStore() *MapStore {
	store := &MapStore{}
	store.data.Store(make(map[int64]int64))
	return store
}

func (s *MapStore) Get(key int64) (int64, bool) {
	m := s.data.Load().(map[int64]int64)

	v, found := m[key]
	return v, found
}

func (s *MapStore) Replace(m map[int64]int64) {
	s.data.Store(m)
}
