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

package wtcache

import (
	"sync"
	"time"
)

const (
	defaultTTL      = 1 * time.Hour
	defaultErrorTTL = 30 * time.Second
)

type CacheImpl struct {
	sync.Mutex
	cache map[string]cacheItem

	successTTL time.Duration
	errorTTL   time.Duration
	fetcher    FetchFunc
	putter     PutFunc
	timefunc   TimeFunc
}

var _ Cache = (*CacheImpl)(nil)

type FetchFunc func(key string) (any, error)
type PutFunc func(key string, value any) error
type TimeFunc func() time.Time

type cacheItem struct {
	value  any
	expiry time.Time
	dirty  bool // indicates if the putter method was successful.
}

type WTCacheOption func(c *CacheImpl)

// WithTTL sets the default TTL for cache items.
func WithTTL(ttl time.Duration) WTCacheOption {
	return func(c *CacheImpl) {
		c.successTTL = ttl
	}
}

// WithErrorTTL sets the TTL for cache items that result in errors.
func WithErrorTTL(ttl time.Duration) WTCacheOption {
	return func(c *CacheImpl) {
		c.errorTTL = ttl
	}
}

// WithFetcher sets the fetcher function for cache misses.
// If the fetcher fails, its error is returned and the cache is
// not updated.
// This function is called with the cache locked, so it should return
// quickly to avoid blocking other cache operations.
func WithFetcher(fetcher FetchFunc) WTCacheOption {
	return func(c *CacheImpl) {
		c.fetcher = fetcher
	}
}

// WithPutter sets the putter function for cache puts.
// This function is called with the cache locked, so it should return
// quickly to avoid blocking other cache operations.
func WithPutter(putter PutFunc) WTCacheOption {
	return func(c *CacheImpl) {
		c.putter = putter
	}
}

// WithTimeFunc sets the time function for cache expiration.
// This is useful for testing.
func WithTimeFunc(timefunc TimeFunc) WTCacheOption {
	return func(c *CacheImpl) {
		c.timefunc = timefunc
	}
}

// NewCache creates a new cache with the given options.
// If no options are provided, the cache has a default TTL of 5 minutes.
// The default time function is time.Now().
func NewCache(options ...WTCacheOption) *CacheImpl {
	c := &CacheImpl{
		cache:      make(map[string]cacheItem),
		successTTL: defaultTTL,
		errorTTL:   defaultErrorTTL,
		timefunc:   time.Now,
	}

	for _, option := range options {
		option(c)
	}

	return c
}

func (c *CacheImpl) Get(key string) (any, error) {
	c.Lock()
	defer c.Unlock()

	item, ok := c.cache[key]
	if ok {
		if item.expiry.Before(c.timefunc()) {
			delete(c.cache, key)
			ok = false
		}
	}
	if ok {
		return item.value, nil
	}
	if !ok && c.fetcher == nil {
		return nil, nil
	}

	value, err := c.fetcher(key)
	if err != nil {
		c.cache[key] = cacheItem{
			value:  nil,
			expiry: c.timefunc().Add(c.errorTTL),
		}
		return nil, err
	}
	c.cache[key] = cacheItem{
		value:  value,
		expiry: c.timefunc().Add(c.successTTL),
	}
	return value, nil
}

func (c *CacheImpl) Put(key string, value any) error {
	c.Lock()
	defer c.Unlock()

	newitem := cacheItem{
		value:  value,
		expiry: c.timefunc().Add(c.successTTL),
		dirty:  false,
	}

	if c.putter != nil {
		newitem.dirty = true
		c.cache[key] = newitem // in case we fail to put, remember this
		err := c.putter(key, value)
		if err != nil {
			return err
		}
		newitem.dirty = false
	}

	c.cache[key] = newitem

	return nil
}

func (c *CacheImpl) Delete(key string) {
	c.Lock()
	defer c.Unlock()

	delete(c.cache, key)
}

func (c *CacheImpl) Clear() {
	c.Lock()
	defer c.Unlock()

	c.cache = make(map[string]cacheItem)
}

func (c *CacheImpl) Dirty() []string {
	c.Lock()
	defer c.Unlock()

	var dirty []string
	for key, item := range c.cache {
		if item.dirty {
			dirty = append(dirty, key)
		}
	}

	return dirty
}
