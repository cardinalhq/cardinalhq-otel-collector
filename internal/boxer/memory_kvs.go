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

package boxer

import (
	"sync"
	"time"
)

// MemoryKVS is a key-value store.
type MemoryKVS struct {
	sync.Mutex
	kvs      map[string]memoryItem
	timefunc TimeFunc
}

type memoryItem struct {
	value []byte
	ttl   time.Time
}

var (
	_ KVS   = (*MemoryKVS)(nil)
	_ Wiper = (*MemoryKVS)(nil)
)

// NewMemoryKVS creates a new MemoryKVS.
func NewMemoryKVS(timefunc TimeFunc) KVS {
	if timefunc == nil {
		timefunc = time.Now
	}
	return &MemoryKVS{
		kvs:      make(map[string]memoryItem),
		timefunc: timefunc,
	}
}

func (m *MemoryKVS) Get(key []byte) ([]byte, error) {
	m.Lock()
	defer m.Unlock()
	strkey := string(key)
	item, ok := m.kvs[strkey]
	if !ok {
		return nil, nil
	}
	if m.expiredItem(item) {
		delete(m.kvs, strkey)
		return nil, nil
	}
	return item.value, nil
}

func (m *MemoryKVS) Set(key []byte, value []byte, ttl time.Duration) error {
	m.Lock()
	defer m.Unlock()
	itemTTL := time.Time{}
	if ttl != 0 {
		itemTTL = m.timefunc().Add(ttl)
	}
	m.kvs[string(key)] = memoryItem{
		value: value,
		ttl:   itemTTL,
	}
	return nil
}

func (m *MemoryKVS) expired(key string) bool {
	item, ok := m.kvs[key]
	if !ok {
		return false
	}
	return m.expiredItem(item)
}

func (m *MemoryKVS) expiredItem(item memoryItem) bool {
	if !item.ttl.IsZero() && item.ttl.Before(m.timefunc()) {
		return true
	}
	return false
}

func (m *MemoryKVS) Delete(key []byte) error {
	m.Lock()
	defer m.Unlock()
	delete(m.kvs, string(key))
	return nil
}

func (m *MemoryKVS) ForEachPrefix(prefix []byte, f func(k []byte, v []byte) bool) error {
	m.Lock()
	defer m.Unlock()
	foundKeys := make(map[string]struct{})
	for k := range m.kvs {
		if len(k) < len(prefix) {
			continue
		}
		if string(k[:len(prefix)]) == string(prefix) {
			foundKeys[k] = struct{}{}
		}
	}

	for k := range foundKeys {
		if m.expired(k) {
			delete(m.kvs, k)
			continue
		}
		if !f([]byte(k), m.kvs[k].value) {
			return nil
		}
	}
	return nil
}

func (m *MemoryKVS) Maintain() error {
	for k := range m.kvs {
		if m.expired(k) {
			delete(m.kvs, k)
		}
	}
	return nil
}

func (m *MemoryKVS) Close() error {
	return nil
}

func (m *MemoryKVS) Wipe() error {
	m.Lock()
	defer m.Unlock()
	m.kvs = make(map[string]memoryItem)
	return nil
}
