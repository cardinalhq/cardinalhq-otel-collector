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
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
)

var (
	NoSuchIntervalError = errors.New("no such interval")
	NoSuchScopeError    = errors.New("no such scope")
)

type BufferRecord struct {
	Scope    string
	Interval int64
	Contents []byte
}

type ForEachFunc func(record *BufferRecord) (keepGoing bool, err error)

type Buffer interface {
	Write(data *BufferRecord) error
	GetScopes(interval int64) (scopes []string, err error)
	GetIntervals() (intervals []int64, err error)
	ForEach(interval int64, scope string, f ForEachFunc) error
	CloseIntervalScope(interval int64, scope string) error
	CloseInterval(interval int64) error
	Shutdown() error
}

var (
	ErrShutdown = errors.New("buffer is shut down")
	WriteError  = errors.New("error writing to buffer")
)

func encodeToFile(f io.Writer, data *BufferRecord) error {
	buff := bytes.Buffer{}
	if err := gob.NewEncoder(&buff).Encode(data); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, int64(buff.Len())); err != nil {
		return err
	}
	if _, err := f.Write(buff.Bytes()); err != nil {
		return err
	}
	return nil
}

func decodeFromFile(f io.Reader) (*BufferRecord, error) {
	var size int64
	if err := binary.Read(f, binary.LittleEndian, &size); err != nil {
		return nil, err
	}
	buff := make([]byte, size)
	if _, err := f.Read(buff); err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(bytes.NewReader(buff))
	var data BufferRecord
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

func iterate(f io.Reader, fn ForEachFunc) error {
	for {
		record, err := decodeFromFile(f)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		keepGoing, err := fn(record)
		if err != nil {
			return err
		}
		if !keepGoing {
			return nil
		}
	}
}
