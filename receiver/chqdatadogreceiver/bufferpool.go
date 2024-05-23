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

package datadogreceiver

import (
	"sync"
	"sync/atomic"
)

type bufferpool struct {
	size        int
	name        string
	pool        sync.Pool
	allocations atomic.Int64
}

type readbuf struct {
	buf []byte
}

var ()

func (bp *bufferpool) Get() *readbuf {
	ret := bp.pool.Get().(*readbuf)
	ret.buf = ret.buf[:cap(ret.buf)] // reset to max capacity
	return ret
}

func (bp *bufferpool) Put(rb *readbuf) {
	bp.pool.Put(rb)
}

// NewBufferPool creates a new buffer pool with the given name and size
// `name` should be a simple name, without any special characters,
// as it is used to create metric names
func NewBufferPool(name string, size int) *bufferpool {
	bp := &bufferpool{
		size: size,
		name: name,
	}

	bp.pool = sync.Pool{
		New: func() interface{} {
			bp.allocations.Add(1)
			return &readbuf{buf: make([]byte, size)}
		},
	}

	return bp
}
