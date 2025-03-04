// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

type ForEachFunc func(index, expected int, record *BufferRecord) (keepGoing bool, err error)

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
	readlen, err := f.Read(buff)
	if err != nil {
		return nil, err
	}
	if int64(readlen) != size {
		return nil, io.ErrUnexpectedEOF
	}
	dec := gob.NewDecoder(bytes.NewReader(buff))
	var data BufferRecord
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

func iterate(f io.Reader, expected int, fn ForEachFunc) error {
	index := 0
	for {
		record, err := decodeFromFile(f)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		keepGoing, err := fn(index, expected, record)
		if err != nil {
			return err
		}
		if !keepGoing {
			return nil
		}
		index++
	}
}
