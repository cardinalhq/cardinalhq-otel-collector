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
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestDB(t *testing.T) *badger.DB {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	return db
}

func TestBadgerKVS_Get(t *testing.T) {
	db := createTestDB(t)
	kvs := NewBadgerKVS(db)

	key := []byte("test-key")
	value := []byte("test-value")
	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	require.NoError(t, err)

	result, err := kvs.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, result)

	result, err = kvs.Get([]byte("non-existent-key"))
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBadgerKVS_Set(t *testing.T) {
	db := createTestDB(t)
	kvs := NewBadgerKVS(db)

	key := []byte("test-key")
	value := []byte("test-value")
	err := kvs.Set(key, value, 0)
	require.NoError(t, err)

	result, err := kvs.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, result)

	value2 := []byte("test-value2")
	err = kvs.Set(key, value2, 0)
	require.NoError(t, err)

	result, err = kvs.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value2, result)
}

func TestBadgerKVS_SetWithTTL(t *testing.T) {
	db := createTestDB(t)
	kvs := NewBadgerKVS(db)

	key := []byte("test-key")
	value := []byte("test-value")
	err := kvs.Set(key, value, 1)
	require.NoError(t, err)

	time.Sleep(100 * time.Microsecond)

	result, err := kvs.Get(key)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBadgerKVS_Delete(t *testing.T) {
	db := createTestDB(t)
	kvs := NewBadgerKVS(db)

	key := []byte("test-key")
	value := []byte("test-value")
	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	require.NoError(t, err)

	err = kvs.Delete(key)
	require.NoError(t, err)

	result, err := kvs.Get(key)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func populate(t *testing.T, db *badger.DB) {
	err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 10; i++ {
			key := []byte{0, byte(i)}
			value := []byte{byte(i)}
			err := txn.Set(key, value)
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestBadgerKVS_ForEachPrefix(t *testing.T) {
	db := createTestDB(t)
	kvs := NewBadgerKVS(db)

	populate(t, db)

	count := 0
	err := kvs.ForEachPrefix([]byte{0}, func(key []byte, value []byte) bool {
		count++
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	count = 0
	err = kvs.ForEachPrefix([]byte{0}, func(key []byte, value []byte) bool {
		count++
		return false
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestBadgerKVS_Wipe(t *testing.T) {
	db := createTestDB(t)
	kvs := NewBadgerKVS(db)

	populate(t, db)

	err := kvs.Wipe()
	require.NoError(t, err)

	count := 0
	err = kvs.ForEachPrefix([]byte{}, func(key []byte, value []byte) bool {
		count++
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestBadgerKVS_Close(t *testing.T) {
	db := createTestDB(t)
	kvs := NewBadgerKVS(db)

	err := kvs.Close()
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		return nil
	})
	require.Error(t, err)
}

func TestBadgerKVS_Maintain(t *testing.T) {
	db := createTestDB(t)
	kvs := NewBadgerKVS(db)

	err := kvs.Maintain()
	require.NoError(t, err)
}
