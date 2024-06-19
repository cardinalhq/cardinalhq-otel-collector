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

package tagwriter

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParquetMapWriter_WriteRows(t *testing.T) {
	rows := []map[string]any{
		{"name": "John", "age": int64(30)},
		{"name": "Jane", "age": int64(25)},
	}

	schema, err := ParquetSchemaFromMap("schema", rows[0])
	assert.NoError(t, err)

	var buf bytes.Buffer
	writer, err := NewParquetMapWriter(&buf, schema)
	assert.NoError(t, err)

	count, err := writer.WriteRows(rows)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if count != len(rows) {
		t.Fatalf("expected %d, got %d", len(rows), count)
	}
}
