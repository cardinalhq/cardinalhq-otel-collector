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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryBuffer_Write(t *testing.T) {
	buffer := NewMemoryBuffer()

	record := &BufferRecord{
		Interval: 123,
		Scope:    "test",
		Contents: []byte("test"),
	}

	err := buffer.Write(record)
	assert.NoError(t, err)

	scopes, err := buffer.GetScopes(123)
	assert.NoError(t, err)
	assert.Equal(t, []string{"test"}, scopes)

	intervals, err := buffer.GetIntervals()
	assert.NoError(t, err)
	assert.Equal(t, []int64{123}, intervals)
}

func TestMemoryBuffer_Write_closed(t *testing.T) {
	buffer := NewMemoryBuffer()
	assert.NoError(t, buffer.Shutdown())

	record := &BufferRecord{
		Interval: 123,
		Scope:    "test",
		Contents: []byte("test")}

	err := buffer.Write(record)
	assert.ErrorIs(t, err, ErrShutdown)
}

func TestMemoryBuffer_GetScopes_GetIntervals(t *testing.T) {
	buffer := NewMemoryBuffer()

	record := &BufferRecord{
		Interval: 123,
		Scope:    "test",
		Contents: []byte("test-123"),
	}

	err := buffer.Write(record)
	assert.NoError(t, err)

	scopes, err := buffer.GetScopes(123)
	assert.NoError(t, err)
	assert.Equal(t, []string{"test"}, scopes)

	intervals, err := buffer.GetIntervals()
	assert.NoError(t, err)
	assert.Equal(t, []int64{123}, intervals)

	record2 := &BufferRecord{
		Interval: 123,
		Scope:    "test2",
		Contents: []byte("test2-123"),
	}

	err = buffer.Write(record2)
	assert.NoError(t, err)

	scopes, err = buffer.GetScopes(123)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"test", "test2"}, scopes)

	record3 := &BufferRecord{
		Interval: 124,
		Scope:    "test",
		Contents: []byte("test-124"),
	}

	err = buffer.Write(record3)
	assert.NoError(t, err)

	scopes, err = buffer.GetScopes(124)
	assert.NoError(t, err)
	assert.Equal(t, []string{"test"}, scopes)

	intervals, err = buffer.GetIntervals()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{123, 124}, intervals)
}

func TestMemoryBuffer_GetScopes_closed(t *testing.T) {
	buffer := NewMemoryBuffer()
	assert.NoError(t, buffer.Shutdown())

	scopes, err := buffer.GetScopes(123)
	assert.ErrorIs(t, err, ErrShutdown)
	assert.Nil(t, scopes)
}

func TestMemoryBuffer_GetIntervals_closed(t *testing.T) {
	buffer := NewMemoryBuffer()
	assert.NoError(t, buffer.Shutdown())

	intervals, err := buffer.GetIntervals()
	assert.ErrorIs(t, err, ErrShutdown)
	assert.Nil(t, intervals)
}

func TestMemoryBuffer_CloseIntervalScope(t *testing.T) {
	buffer := NewMemoryBuffer()

	record := &BufferRecord{
		Interval: 123,
		Scope:    "test",
		Contents: []byte("test"),
	}

	err := buffer.Write(record)
	assert.NoError(t, err)

	err = buffer.CloseIntervalScope(123, "test")
	assert.NoError(t, err)

	scopes, err := buffer.GetScopes(123)
	assert.NoError(t, err)
	assert.Empty(t, scopes)

	intervals, err := buffer.GetIntervals()
	assert.NoError(t, err)
	assert.Empty(t, intervals)
}

func TestMemoryBuffer_CloseIntervalScope_closed(t *testing.T) {
	buffer := NewMemoryBuffer()
	assert.NoError(t, buffer.Shutdown())

	err := buffer.CloseIntervalScope(123, "test")
	assert.ErrorIs(t, err, ErrShutdown)
}

func TestMemoryBuffer_CloseIntervalScope_notFound(t *testing.T) {
	buffer := NewMemoryBuffer()

	err := buffer.CloseIntervalScope(123, "test")
	assert.NoError(t, err)
}

func TestMemoryBuffer_Shutdown(t *testing.T) {
	buffer := NewMemoryBuffer()

	record := &BufferRecord{
		Interval: 123,
		Scope:    "test",
		Contents: []byte("test"),
	}

	err := buffer.Write(record)
	assert.NoError(t, err)

	assert.NoError(t, buffer.Shutdown())

	err = buffer.Write(record)
	assert.ErrorIs(t, err, ErrShutdown)
}

func TestMemoryBuffer_ForEach(t *testing.T) {
	records := []*BufferRecord{
		{
			Interval: 123,
			Scope:    "test",
			Contents: []byte("test-123"),
		},
		{
			Interval: 123,
			Scope:    "test",
			Contents: []byte("test-123-part2"),
		},
		{
			Interval: 123,
			Scope:    "test2",
			Contents: []byte("test2-123"),
		},
		{
			Interval: 124,
			Scope:    "test",
			Contents: []byte("test-124"),
		},
		{
			Interval: 125,
			Scope:    "test9",
			Contents: []byte("test9-125"),
		},
	}

	buffer := NewMemoryBuffer()

	for _, record := range records {
		err := buffer.Write(record)
		assert.NoError(t, err)
	}

	var results []*BufferRecord
	err := buffer.ForEach(123, "test", func(index, expected int, record *BufferRecord) (bool, error) {
		assert.Equal(t, len(results), index)
		results = append(results, record)
		assert.Equal(t, 2, expected)
		return true, nil
	})
	assert.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, records[0], results[0])
	assert.Equal(t, records[1], results[1])

	// test for early exit
	results = nil
	err = buffer.ForEach(123, "test", func(index, expected int, record *BufferRecord) (bool, error) {
		results = append(results, record)
		return false, nil
	})
	assert.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, records[0], results[0])

	results = nil
	err = buffer.ForEach(123, "test2", func(index, expected int, record *BufferRecord) (bool, error) {
		results = append(results, record)
		return true, nil
	})
	assert.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, records[2], results[0])

	results = nil
	err = buffer.ForEach(124, "test", func(index, expected int, record *BufferRecord) (bool, error) {
		results = append(results, record)
		return true, nil
	})
	assert.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, records[3], results[0])

	results = nil
	err = buffer.ForEach(125, "testnotthere", func(index, expected int, record *BufferRecord) (bool, error) {
		results = append(results, record)
		return true, nil
	})
	assert.ErrorIs(t, err, NoSuchScopeError)
	assert.Empty(t, results)

	results = nil
	err = buffer.ForEach(999, "test", func(index, expected int, record *BufferRecord) (bool, error) {
		results = append(results, record)
		return true, nil
	})
	assert.ErrorIs(t, err, NoSuchIntervalError)
	assert.Empty(t, results)
}
