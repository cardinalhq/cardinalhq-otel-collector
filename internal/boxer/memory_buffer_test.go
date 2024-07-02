package boxer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryBuffer_Write(t *testing.T) {
	buffer := NewMemoryBuffer()

	record := &BufferRecord{
		Interval: 123,
		Scope:    "test",
		// Set other fields of the record as needed
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
	buffer.Shutdown()

	record := &BufferRecord{
		Interval: 123,
		Scope:    "test",
		// Set other fields of the record as needed
	}

	err := buffer.Write(record)
	assert.ErrorIs(t, err, ErrShutdown)
}

func TestMemoryBuffer_GetScopes_closed(t *testing.T) {
	buffer := NewMemoryBuffer()
	buffer.Shutdown()

	scopes, err := buffer.GetScopes(123)
	assert.ErrorIs(t, err, ErrShutdown)
	assert.Nil(t, scopes)
}

func TestMemoryBuffer_GetIntervals_closed(t *testing.T) {
	buffer := NewMemoryBuffer()
	buffer.Shutdown()

	intervals, err := buffer.GetIntervals()
	assert.ErrorIs(t, err, ErrShutdown)
	assert.Nil(t, intervals)
}

func TestMemoryBuffer_CloseInterval(t *testing.T) {
	buffer := NewMemoryBuffer()

	record := &BufferRecord{
		Interval: 123,
		Scope:    "test",
		// Set other fields of the record as needed
	}

	err := buffer.Write(record)
	assert.NoError(t, err)

	err = buffer.CloseInterval(123, "test")
	assert.NoError(t, err)

	scopes, err := buffer.GetScopes(123)
	assert.NoError(t, err)
	assert.Empty(t, scopes)

	intervals, err := buffer.GetIntervals()
	assert.NoError(t, err)
	assert.Empty(t, intervals)
}
