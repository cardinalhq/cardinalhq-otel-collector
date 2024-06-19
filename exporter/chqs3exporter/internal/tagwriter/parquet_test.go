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
