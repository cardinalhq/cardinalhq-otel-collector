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
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMemoryKVS_with_timefunc(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvsi := NewMemoryKVS(timefunc)
	kvs := kvsi.(*MemoryKVS)
	assert.NotNil(t, kvs)
	assert.NotNil(t, kvs.kvs)
	assert.NotNil(t, kvs.timefunc)
}

func TestNewMemoryKVS_without_timefunc(t *testing.T) {
	kvsi := NewMemoryKVS(nil)
	kvs := kvsi.(*MemoryKVS)
	assert.NotNil(t, kvs)
	assert.NotNil(t, kvs.kvs)
	assert.NotNil(t, kvs.timefunc)
}

func TestMemoryKVS_Get_no_ttl(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)

	key := []byte("key")
	value := []byte("value")
	err := kvs.Set(key, value, 0)
	assert.NoError(t, err)

	v, err := kvs.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, v)
}

func TestMemoryKVS_Get_ttl_expired(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvsi := NewMemoryKVS(timefunc)
	kvs := kvsi.(*MemoryKVS)

	key := []byte("key")
	value := []byte("value")
	err := kvs.Set(key, value, time.Second)
	assert.NoError(t, err)

	timefunc = func() time.Time { return time.Unix(1001, 1001) }
	kvs.timefunc = timefunc

	v, err := kvs.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, v)
}

func TestMemoryKVS_Get_ttl_not_expired(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)

	key := []byte("key")
	value := []byte("value")
	err := kvs.Set(key, value, time.Second)
	assert.NoError(t, err)

	v, err := kvs.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, v)
}

func TestMemoryKVS_Set_no_ttl(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvsi := NewMemoryKVS(timefunc)
	kvs := kvsi.(*MemoryKVS)

	key := []byte("key")
	value := []byte("value")
	err := kvs.Set(key, value, 0)
	assert.NoError(t, err)
	assert.True(t, kvs.kvs[string(key)].ttl.IsZero())
}

func TestMemoryKVS_expired(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvsi := NewMemoryKVS(timefunc)
	kvs := kvsi.(*MemoryKVS)

	key := "key"
	item := memoryItem{
		value: []byte("value"),
		ttl:   time.Unix(1001, 1001),
	}
	kvs.kvs[key] = item
	assert.False(t, kvs.expired(key))

	timefunc = func() time.Time { return time.Unix(1002, 1002) }
	kvs.timefunc = timefunc
	assert.True(t, kvs.expired(key))
}

func TestMemoryKVS_expiredItem(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvsi := NewMemoryKVS(timefunc)
	kvs := kvsi.(*MemoryKVS)

	item := memoryItem{
		value: []byte("value"),
		ttl:   time.Unix(1001, 1001),
	}
	assert.False(t, kvs.expiredItem(item))

	timefunc = func() time.Time { return time.Unix(1002, 1002) }
	kvs.timefunc = timefunc
	assert.True(t, kvs.expiredItem(item))
}

func TestMemoryKVS_expired_does_not_exist(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvsi := NewMemoryKVS(timefunc)
	kvs := kvsi.(*MemoryKVS)
	assert.False(t, kvs.expired("key"))
}

func TestMemoryKVS_Delete(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)

	key := []byte("key")
	value := []byte("value")
	err := kvs.Set(key, value, 0)
	assert.NoError(t, err)

	err = kvs.Delete(key)
	assert.NoError(t, err)

	v, err := kvs.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, v)
}

func TestMemoryKVS_Delete_not_exist(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)

	key := []byte("key")
	err := kvs.Delete(key)
	assert.NoError(t, err)
}

func TestMemoryKVS_Maintain(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)

	// will be expired
	key1 := []byte("key1")
	value1 := []byte("value1")
	err := kvs.Set(key1, value1, time.Duration(-time.Hour))
	assert.NoError(t, err)

	// no ttl
	key2 := []byte("key2")
	value2 := []byte("value2")
	err = kvs.Set(key2, value2, 0)
	assert.NoError(t, err)

	// future ttl will not be expired
	key3 := []byte("key3")
	value3 := []byte("value3")
	err = kvs.Set(key3, value3, time.Second)
	assert.NoError(t, err)

	err = kvs.Maintain()
	assert.NoError(t, err)

	v, err := kvs.Get(key1)
	assert.NoError(t, err)
	assert.Nil(t, v)

	v, err = kvs.Get(key2)
	assert.NoError(t, err)
	assert.Equal(t, value2, v)

	v, err = kvs.Get(key3)
	assert.NoError(t, err)
	assert.Equal(t, value3, v)
}

func TestMemoryKVS_Close(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)
	assert.NoError(t, kvs.Close())
}

func TestMemoryKVS_ForEachPrefix(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)

	// key1 will expire during the iteration
	key1 := []byte("key1")
	value1 := []byte("value1")
	err := kvs.Set(key1, value1, time.Duration(-time.Hour))
	assert.NoError(t, err)

	key2 := []byte("key2")
	value2 := []byte("value2")
	err = kvs.Set(key2, value2, 0)
	assert.NoError(t, err)

	key3 := []byte("key3")
	value3 := []byte("value3")
	err = kvs.Set(key3, value3, time.Second)
	assert.NoError(t, err)

	// will not be found
	notKey := []byte("a")
	notValue := []byte("notValue")
	err = kvs.Set(notKey, notValue, 0)
	assert.NoError(t, err)

	keys := [][]byte{key2, key3}
	values := [][]byte{value2, value3}

	// test stop iteration
	err = kvs.ForEachPrefix([]byte("k"), func(k []byte, v []byte) bool {
		assert.True(t, bytes.Equal(k, key2) || bytes.Equal(k, key3), fmt.Sprintf("Got key %s", string(k)))
		assert.True(t, bytes.Equal(v, value2) || bytes.Equal(v, value3), fmt.Sprintf("Got value %s", string(v)))
		return false
	})
	assert.NoError(t, err)

	// test full iteration
	foundKeys := make([][]byte, 0)
	foundValues := make([][]byte, 0)
	err = kvs.ForEachPrefix([]byte("k"), func(k []byte, v []byte) bool {
		foundKeys = append(foundKeys, k)
		foundValues = append(foundValues, v)
		return true
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, keys, foundKeys)
	assert.ElementsMatch(t, values, foundValues)
}

func TestMemoryKVS_Wipe(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)

	key := []byte("key")
	value := []byte("value")
	err := kvs.Set(key, value, 0)
	assert.NoError(t, err)

	if wiper, ok := kvs.(Wiper); !ok {
		t.Fatalf("MemoryKVS does not implement Wiper")
	} else {
		assert.NoError(t, wiper.Wipe())
	}

	v, err := kvs.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, v)
}
