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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeToFile(t *testing.T) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "encode-test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Create a sample BufferRecord
	record := &BufferRecord{
		Scope:    "test",
		Interval: 123,
		Contents: []byte("test-123"),
	}

	// Encode the record to the file
	err = encodeToFile(tempFile, record)
	assert.NoError(t, err)

	// Read the encoded data from the file
	fileData, err := os.ReadFile(tempFile.Name())
	assert.NoError(t, err)

	// Decode the data from the file
	decodedRecord, err := decodeFromFile(bytes.NewReader(fileData))
	assert.NoError(t, err)

	// Verify that the decoded record matches the original record
	assert.Equal(t, record, decodedRecord)
}
