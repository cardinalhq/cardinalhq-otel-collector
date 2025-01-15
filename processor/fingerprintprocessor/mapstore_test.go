package fingerprintprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMapStore(t *testing.T) {
	store := NewMapStore()
	require.NotNil(t, store)

	data := store.data.Load()
	require.NotNil(t, data)

	require.IsType(t, map[int64]int64{}, data)
}

func TestMapStore_Get(t *testing.T) {
	store := NewMapStore()
	testKey := int64(1)
	testValue := int64(42)

	// Initialize the map with a test key-value pair
	initialMap := map[int64]int64{testKey: testValue}
	store.data.Store(initialMap)

	// Test if Get returns the correct value for the existing key
	value, found := store.Get(testKey)
	assert.Equal(t, value, int64(42))
	assert.True(t, found)

	// Test if Get returns 0 for a non-existing key
	nonExistingKey := int64(2)
	value, found = store.Get(nonExistingKey)
	assert.Equal(t, value, int64(0))
	assert.False(t, found)
}

func TestMapStore_Replace(t *testing.T) {
	store := NewMapStore()

	// Initial map to replace
	initialMap := map[int64]int64{1: 42, 2: 84}
	store.Replace(initialMap)

	// Verify that the map was replaced
	data := store.data.Load().(map[int64]int64)
	if len(data) != len(initialMap) {
		t.Fatalf("Expected map length %d, got %d", len(initialMap), len(data))
	}
	for k, v := range initialMap {
		if data[k] != v {
			t.Fatalf("Expected value %d for key %d, got %d", v, k, data[k])
		}
	}

	// Replace with a new map
	newMap := map[int64]int64{3: 126, 4: 168}
	store.Replace(newMap)

	// Verify that the map was replaced
	data = store.data.Load().(map[int64]int64)
	if len(data) != len(newMap) {
		t.Fatalf("Expected map length %d, got %d", len(newMap), len(data))
	}
	for k, v := range newMap {
		if data[k] != v {
			t.Fatalf("Expected value %d for key %d, got %d", v, k, data[k])
		}
	}

	// Replace with the same map to check if it doesn't update
	store.Replace(newMap)
}

func BenchmarkReplace(b *testing.B) {
	store := NewMapStore()
	newMap := map[int64]int64{1: 42, 2: 84}

	for i := 0; i < b.N; i++ {
		store.Replace(newMap)
	}
}

func BenchmarkGet(b *testing.B) {
	store := NewMapStore()
	newMap := map[int64]int64{1: 42, 2: 84}
	store.Replace(newMap)

	b.Run("using MapStore", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store.Get(1)
		}
	})

	b.Run("direct access", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = newMap[1]
		}
	})
}
