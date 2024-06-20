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

package timebox

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferFilesystem_Write(t *testing.T) {
	file, err := os.CreateTemp("", "buffer_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	buffer, err := NewBufferFilesystem(file)
	require.NoError(t, err)

	data := []byte("test data")
	n, err := buffer.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	readData := make([]byte, len(data))
	_, err = file.Seek(0, 0)
	require.NoError(t, err)

	_, err = file.Read(readData)
	require.NoError(t, err)
	assert.Equal(t, data, readData)
}

func TestBufferFilesystem_Read(t *testing.T) {
	file, err := os.CreateTemp("", "buffer_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	data := []byte("test data")
	_, err = file.Write(data)
	require.NoError(t, err)

	buffer, err := NewBufferFilesystem(file)
	require.NoError(t, err)

	readData := make([]byte, len(data))
	n, err := buffer.Read(readData)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, readData)
}

func TestBufferFilesystem_Close(t *testing.T) {
	file, err := os.CreateTemp("", "buffer_test")
	require.NoError(t, err)

	buffer, err := NewBufferFilesystem(file)
	require.NoError(t, err)

	err = buffer.Close()
	require.NoError(t, err)
}

func TestBufferFilesystem_Cleanup(t *testing.T) {
	file, err := os.CreateTemp("", "buffer_test")
	require.NoError(t, err)

	buffer, err := NewBufferFilesystem(file)
	require.NoError(t, err)

	err = buffer.Cleanup()
	require.NoError(t, err)

	_, err = os.Stat(file.Name())
	assert.True(t, os.IsNotExist(err))
}

func TestMixedWriteAndRead(t *testing.T) {
	file, err := os.CreateTemp("", "buffer_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	buffer, err := NewBufferFilesystem(file)
	require.NoError(t, err)

	data := []byte("test data")
	n, err := buffer.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	readData := make([]byte, len(data))
	n, err = buffer.Read(readData)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, readData)

	data = []byte("more data")
	n, err = buffer.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	readData = make([]byte, len(data))
	n, err = buffer.Read(readData)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, readData)
}

func TestBufferFilesystemFactory_NewBuffer(t *testing.T) {
	factory := NewBufferFilesystemFactory("")
	buffer, err := factory.NewBuffer()
	require.NoError(t, err)
	assert.NotNil(t, buffer)
	assert.IsType(t, &BufferFilesystem{}, buffer)
}
