package chqstatsprocessor

import (
	"container/list"
	"sync"
)

type LRUCache struct {
	capacity int
	cache    map[int64]*list.Element
	list     *list.List
	mutex    sync.RWMutex
}

type Entry struct {
	key   int64
	value interface{}
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[int64]*list.Element),
		list:     list.New(),
	}
}

func (l *LRUCache) Contains(key int64) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	_, found := l.cache[key]
	return found
}

func (l *LRUCache) Get(key int64) (interface{}, bool) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if elem, found := l.cache[key]; found {
		l.mutex.RUnlock()
		l.mutex.Lock()
		l.list.MoveToFront(elem)
		l.mutex.Unlock()
		l.mutex.RLock() // Reacquire read lock for safe return
		return elem.Value.(*Entry).value, true
	}
	return nil, false
}

func (l *LRUCache) Put(key int64, value interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, found := l.cache[key]; found {
		elem.Value.(*Entry).value = value
		l.list.MoveToFront(elem)
		return
	}

	// If the cache is at capacity, remove the least recently used item.
	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			l.list.Remove(back)
			delete(l.cache, back.Value.(*Entry).key)
		}
	}

	newEntry := &Entry{key: key, value: value}
	elem := l.list.PushFront(newEntry)
	l.cache[key] = elem
}

func (l *LRUCache) Remove(key int64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, found := l.cache[key]; found {
		l.list.Remove(elem)
		delete(l.cache, key)
	}
}
