package boxer

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"sync"

	"golang.org/x/exp/maps"
)

type MemoryBuffer struct {
	sync.Mutex
	records  map[int64]map[string]bytes.Buffer
	shutdown bool
}

var (
	_ Buffer = &MemoryBuffer{}

	ErrShutdown = errors.New("buffer is shut down")
)

func NewMemoryBuffer() *MemoryBuffer {
	return &MemoryBuffer{
		records: make(map[int64]map[string]bytes.Buffer),
	}
}

func (b *MemoryBuffer) Write(data *BufferRecord) error {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return ErrShutdown
	}

	buff := bytes.Buffer{}

	if _, ok := b.records[data.Interval]; !ok {
		b.records[data.Interval] = make(map[string]bytes.Buffer)
	}
	if _, ok := b.records[data.Interval][data.Scope]; !ok {
		b.records[data.Interval][data.Scope] = bytes.Buffer{}
	}
	appendBuf := b.records[data.Interval][data.Scope]
	err := gob.NewEncoder(&buff).Encode(data)
	if err != nil {
		return err
	}
	binary.Write(&appendBuf, binary.LittleEndian, int64(buff.Len()))
	binary.Write(&appendBuf, binary.LittleEndian, buff.Bytes())
	return nil
}

func (b *MemoryBuffer) GetScopes(interval int64) (scopes []string, err error) {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return nil, ErrShutdown
	}

	return maps.Keys(b.records[interval]), nil
}

func (b *MemoryBuffer) GetIntervals() (intervals []int64, err error) {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return nil, ErrShutdown
	}

	return maps.Keys(b.records), nil
}

func (b *MemoryBuffer) ForEach(interval int64, scope string, f ForEachFunc) error {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return ErrShutdown
	}

	buf := b.records[interval][scope]
	for buf.Len() > 0 {
		var size int64
		err := binary.Read(&buf, binary.LittleEndian, &size)
		if err != nil {
			return err
		}
		out := BufferRecord{}
		err = gob.NewDecoder(&buf).Decode(&out)
		if err != nil {
			return err
		}
		keepGoing, err := f(&out)
		if err != nil {
			return err
		}
		if !keepGoing {
			break
		}
	}
	return nil
}

func (b *MemoryBuffer) CloseInterval(interval int64, scope string) error {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return ErrShutdown
	}

	delete(b.records[interval], scope)
	if len(b.records[interval]) == 0 {
		delete(b.records, interval)
	}
	return nil
}

func (b *MemoryBuffer) Shutdown() error {
	b.Lock()
	defer b.Unlock()
	b.shutdown = true
	b.records = nil
	return nil
}
