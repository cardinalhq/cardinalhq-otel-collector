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
