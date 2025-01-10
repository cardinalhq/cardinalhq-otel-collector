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
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeToFile(t *testing.T) {
	tempFile, err := os.CreateTemp("", "encode-test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	record := &BufferRecord{
		Scope:    "test",
		Interval: 123,
		Contents: []byte("test-123"),
	}

	for i := 0; i < 100; i++ {
		err = encodeToFile(tempFile, record)
		assert.NoError(t, err)
	}

	_, err = tempFile.Seek(0, 0)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		decodedRecord, err := decodeFromFile(tempFile)
		assert.NoError(t, err)
		assert.Equal(t, record, decodedRecord)
	}

	// Try to read one more record, which should fail with io.EOF
	_, err = decodeFromFile(tempFile)
	assert.ErrorIs(t, err, io.EOF)
}

func TestIterate(t *testing.T) {
	tempFile, err := os.CreateTemp("", "iterate-test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	record := &BufferRecord{
		Scope:    "test",
		Interval: 123,
		Contents: []byte("test-123"),
	}

	for i := 0; i < 100; i++ {
		err = encodeToFile(tempFile, record)
		assert.NoError(t, err)
	}

	_, err = tempFile.Seek(0, 0)
	assert.NoError(t, err)

	count := 0
	err = iterate(tempFile, 100, func(index, expected int, record *BufferRecord) (bool, error) {
		assert.Equal(t, record, record)
		assert.Equal(t, index, count)
		assert.Equal(t, expected, 100)
		count++
		return true, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 100, count)
}

func TestIterate_returnFalse(t *testing.T) {
	tempFile, err := os.CreateTemp("", "iterate-test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	record := &BufferRecord{
		Scope:    "test",
		Interval: 123,
		Contents: []byte("test-123"),
	}

	for i := 0; i < 100; i++ {
		err = encodeToFile(tempFile, record)
		assert.NoError(t, err)
	}

	_, err = tempFile.Seek(0, 0)
	assert.NoError(t, err)

	count := 0
	err = iterate(tempFile, 100, func(index, expected int, record *BufferRecord) (bool, error) {
		assert.Equal(t, record, record)
		assert.Equal(t, index, count)
		assert.Equal(t, expected, 100)
		count++
		if count > 3 {
			return false, nil
		}
		return true, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, count)
}

func TestIterate_error(t *testing.T) {
	tempFile, err := os.CreateTemp("", "iterate-test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	record := &BufferRecord{
		Scope:    "test",
		Interval: 123,
		Contents: []byte("test-123"),
	}

	for i := 0; i < 100; i++ {
		err = encodeToFile(tempFile, record)
		assert.NoError(t, err)
	}

	_, err = tempFile.Seek(0, 0)
	assert.NoError(t, err)

	count := 0
	err = iterate(tempFile, 100, func(index, expected int, record *BufferRecord) (bool, error) {
		assert.Equal(t, record, record)
		assert.Equal(t, index, count)
		assert.Equal(t, expected, 100)
		count++
		if count > 3 {
			return false, assert.AnError
		}
		return true, nil
	})
	assert.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 4, count)
}
