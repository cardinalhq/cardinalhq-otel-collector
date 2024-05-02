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
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

type ParquetMapWriter struct {
	writer   *parquet.GenericWriter[map[string]any]
	filename string
	tmpname  string
}

var (
	_ MapWriter = (*ParquetMapWriter)(nil)
)

// NewParquetMapWriter creates a new ParquetMapWriter for the given filename and schema.
// The filename will be used to write the final parquet file, and a temporary file will be created
// with the same name but with a .tmp extension. This temporary file will be used to write the
// parquet data to, and will be renamed to the final filename when Close is called.
func NewParquetMapWriter(filename string, schema *parquet.Schema) (*ParquetMapWriter, error) {
	tmpname := filename + ".tmp"
	f, err := os.Create(tmpname)
	if err != nil {
		return nil, fmt.Errorf("error creating file: %v", err)
	}
	wc, err := parquet.NewWriterConfig(schema, parquet.Compression(&parquet.Zstd))
	if err != nil {
		return nil, fmt.Errorf("error creating writer config: %v", err)
	}
	writer := parquet.NewGenericWriter[map[string]any](f, wc)
	return &ParquetMapWriter{writer: writer, filename: filename, tmpname: tmpname}, nil
}

// WriteRows writes the given rows to the parquet file.
func (w *ParquetMapWriter) WriteRows(rows []map[string]any) (count int, err error) {
	return w.writer.Write(rows)
}

// Close closes the writer and renames the temporary file to the final filename.
func (w *ParquetMapWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %v", err)
	}
	if err := os.Rename(w.tmpname, w.filename); err != nil {
		return fmt.Errorf("error renaming file: %v", err)
	}
	return nil
}

// Abort closes the writer and removes the temporary file.
func (w *ParquetMapWriter) Abort() error {
	_ = w.writer.Close()
	// ignore error here, as we still want to remove the file.
	if err := os.Remove(w.tmpname); err != nil {
		return fmt.Errorf("error removing file: %v", err)
	}
	return nil
}

// ParquetNodeFromType returns a parquet.Node for the given Go type.
// Not all types are supported.
func ParquetNodeFromType(t any) (parquet.Node, error) {
	switch t.(type) {
	case int8, byte:
		return parquet.Required(parquet.Int(8)), nil
	case int16:
		return parquet.Required(parquet.Int(16)), nil
	case int32, int:
		return parquet.Required(parquet.Int(32)), nil
	case int64:
		return parquet.Required(parquet.Int(64)), nil
	case float64, float32:
		return parquet.Required(parquet.Leaf(parquet.DoubleType)), nil
	case string:
		return parquet.Required(parquet.String()), nil
	case bool:
		return parquet.Required(parquet.Leaf(parquet.BooleanType)), nil
	default:
		return nil, fmt.Errorf("unsupported type %T", t)
	}
}

// ParquetSchemaFromMap returns a parquet.Schema for the given map of field names to Go types.
// This is a helper where an exemplar value in a map will be used to generate the schema.
func ParquetSchemaFromMap(name string, typemap map[string]any) (*parquet.Schema, error) {
	fields := map[string]parquet.Node{}
	for name, t := range typemap {
		node, err := ParquetNodeFromType(t)
		if err != nil {
			return nil, err
		}
		fields[name] = node
	}
	return parquet.NewSchema(name, parquet.Group(fields)), nil
}

func ParquetSchameFromNodemap(name string, fields map[string]parquet.Node) (*parquet.Schema, error) {
	return parquet.NewSchema(name, parquet.Group(fields)), nil
}
