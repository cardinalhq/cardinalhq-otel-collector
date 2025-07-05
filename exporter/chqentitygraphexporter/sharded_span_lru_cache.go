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

package chqentitygraphexporter

import (
	"encoding/binary"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const defaultShardCount = 2
const capacityPerShard = 10000

type ShardedSpanLRUCache struct {
	shards []*SpanLRUCache
}

func NewShardedSpanLRUCache(expiry, reportInterval time.Duration, publishCallback func([]*SpanEntry)) *ShardedSpanLRUCache {
	shards := make([]*SpanLRUCache, defaultShardCount)
	for i := 0; i < defaultShardCount; i++ {
		shards[i] = NewSpanLRUCache(capacityPerShard, expiry, reportInterval, publishCallback)
	}
	return &ShardedSpanLRUCache{
		shards: shards,
	}
}

func (s *ShardedSpanLRUCache) shardIndex(traceID pcommon.TraceID) int {
	b := []byte(traceID.String())
	return int(binary.BigEndian.Uint64(b[8:])) % len(s.shards)
}

func (s *ShardedSpanLRUCache) Put(traceID pcommon.TraceID, key int64, attributes []string, fingerprint int64, exemplar ptrace.Traces) {
	idx := s.shardIndex(traceID)
	s.shards[idx].Put(key, fingerprint, exemplar, attributes)
}

func (s *ShardedSpanLRUCache) PutWithShardIndex(shardIndex int, key int64, attributes []string, fingerprint int64, exemplar ptrace.Traces) {
	s.shards[shardIndex].Put(key, fingerprint, exemplar, attributes)
}

func (s *ShardedSpanLRUCache) Contains(traceID pcommon.TraceID, key int64) (bool, int) {
	idx := s.shardIndex(traceID)
	ok := s.shards[idx].ContainsByKey(key)
	return ok, idx
}

func (s *ShardedSpanLRUCache) Close() {
	for _, shard := range s.shards {
		shard.Close()
	}
}
