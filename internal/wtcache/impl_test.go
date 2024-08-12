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

package wtcache

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCacheImpl_Get_nofetcher(t *testing.T) {
	cache := NewCache()

	// Test case 1: Key not found in cache
	value, err := cache.Get("key1")
	assert.Nil(t, value)
	assert.Nil(t, err)

	// Test case 2: Key found in cache, not expired
	cache.cache["key2"] = cacheItem{
		value:  "value2",
		expiry: time.Now().Add(1 * time.Hour),
	}
	value, err = cache.Get("key2")
	assert.Equal(t, "value2", value)
	assert.Nil(t, err)

	// Test case 3: Key found in cache, expired
	cache.cache["key3"] = cacheItem{
		value:  "value3",
		expiry: time.Now().Add(-1 * time.Hour),
	}
	value, err = cache.Get("key3")
	assert.Nil(t, value)
	assert.Nil(t, err)

	// Test case 4: Key not found in cache, fetcher function not set
	cache.fetcher = nil
	value, err = cache.Get("key4")
	assert.Nil(t, value)
	assert.Nil(t, err)

	// Test case 5: Key not found in cache, fetcher function set
	cache.fetcher = func(key string) (any, error) {
		return "value5", nil
	}
	value, err = cache.Get("key5")
	assert.Equal(t, "value5", value)
	assert.Nil(t, err)

	// Test case 6: Key not found in cache, fetcher function returns error
	cache.fetcher = func(key string) (any, error) {
		return nil, errors.New("fetch error")
	}
	value, err = cache.Get("key6")
	assert.Nil(t, value)
	assert.EqualError(t, err, "fetch error")
}

func TestCacheImpl_Get_fetcher(t *testing.T) {
	cache := NewCache()

	// Test case 1: Key not found in cache, fetcher function set
	cache.fetcher = func(key string) (any, error) {
		return "value1", nil
	}
	value, err := cache.Get("key1")
	assert.Equal(t, "value1", value)
	assert.Nil(t, err)

	// Test case 2: Key found in cache, not expired
	cache.cache["key2"] = cacheItem{
		value:  "value2",
		expiry: time.Now().Add(1 * time.Hour),
	}
	cache.fetcher = func(key string) (any, error) {
		return "value2", nil
	}
	value, err = cache.Get("key2")
	assert.Equal(t, "value2", value)
	assert.Nil(t, err)

	// Test case 3: Key found in cache, expired
	cache.cache["key3"] = cacheItem{
		value:  "value3",
		expiry: time.Now().Add(-1 * time.Hour),
	}
	cache.fetcher = func(key string) (any, error) {
		return "value3", nil
	}
	value, err = cache.Get("key3")
	assert.Equal(t, "value3", value)
	assert.Nil(t, err)

	// Test case 4: Key not found in cache, fetcher function returns error
	cache.fetcher = func(key string) (any, error) {
		return nil, errors.New("fetch error")
	}
	value, err = cache.Get("key4")
	assert.Nil(t, value)
	assert.EqualError(t, err, "fetch error")
}

func TestCacheImpl_Put(t *testing.T) {
	cache := NewCache()

	// add a few values
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	cache.Put("key3", "value3")

	// Test case 1: Key found in cache, not expired
	cache.cache["key1"] = cacheItem{
		value:  "value1",
		expiry: time.Now().Add(1 * time.Hour),
	}
	cache.Put("key1", "value1.1")
	value, err := cache.Get("key1")
	assert.Equal(t, "value1.1", value)
	assert.Nil(t, err)

	// Test case 2: Key found in cache, expired
	cache.cache["key2"] = cacheItem{
		value:  "value2",
		expiry: time.Now().Add(-1 * time.Hour),
	}
	cache.Put("key2", "value2.1")
	value, err = cache.Get("key2")
	assert.Equal(t, "value2.1", value)
	assert.Nil(t, err)
}

func TestCacheImpl_Put_putter(t *testing.T) {
	cache := NewCache()

	called := false
	// Test case 1: Key not found in cache, putter function set, no error
	cache.putter = func(key string, value any) error {
		called = true
		return nil
	}
	err := cache.Put("key1", "value1")
	assert.Nil(t, err)
	assert.True(t, called)

	// Test case 2: Key not found in cache, putter function set, error
	cache.putter = func(key string, value any) error {
		return errors.New("put error")
	}
	err = cache.Put("key2", "value2")
	assert.EqualError(t, err, "put error")
	assert.Equal(t, true, cache.cache["key2"].dirty)

	// dirty returns the keys for dirty items
	assert.ElementsMatch(t, []string{"key2"}, cache.Dirty())
}

func TestCacheImpl_Delete(t *testing.T) {
	cache := NewCache()

	// add a few values
	cache.Put("key1", "value1")

	// Test case 1: Key found in cache
	cache.Delete("key1")
	value, err := cache.Get("key1")
	assert.Nil(t, value)
	assert.Nil(t, err)

	// Test case 2: Key not found in cache
	cache.Delete("key2") // doesn't crash test
}

func TestCacheImpl_Clear(t *testing.T) {
	cache := NewCache()

	// add a few values
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	cache.Put("key3", "value3")

	// Test case 1: Clear cache
	cache.Clear()
	assert.Empty(t, cache.cache)
}

func statictime() time.Time {
	return time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
}

func TestCacheImple_WithTTL(t *testing.T) {
	cache := NewCache(WithTTL(123*time.Hour), WithTimeFunc(statictime))

	// add a value
	cache.Put("key1", "value1")
	item := cache.cache["key1"]
	assert.Equal(t, 123*time.Hour, item.expiry.Sub(statictime()))
}

func TestCacheImple_WithFetcher(t *testing.T) {
	cache := NewCache(WithFetcher(func(key string) (any, error) {
		return "value1", nil
	}))

	// Test case 1: Key not found in cache, fetcher function set
	value, err := cache.Get("key1")
	assert.Equal(t, "value1", value)
	assert.Nil(t, err)
}

func TestCacheImple_WithPutter(t *testing.T) {
	cache := NewCache(WithPutter(func(key string, value any) error {
		return nil
	}))

	// Test case 1: Key not found in cache, putter function set
	err := cache.Put("key1", "value1")
	assert.Nil(t, err)
}

func failfetcher(key string) (any, error) {
	return nil, errors.New("fetch error")
}

func TestCacheImple_WithErrorTTL(t *testing.T) {
	cache := NewCache(WithFetcher(failfetcher), WithErrorTTL(123*time.Hour), WithTimeFunc(statictime))

	value, err := cache.Get("key1")
	assert.Nil(t, value)
	assert.EqualError(t, err, "fetch error")

	item := cache.cache["key1"]
	assert.Equal(t, 123*time.Hour, item.expiry.Sub(statictime()))
	assert.Nil(t, item.value)
}
