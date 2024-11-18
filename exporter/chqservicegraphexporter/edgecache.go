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

package chqservicegraphexporter

import (
	"sync"
	"time"
)

// Edge represents an edge in the system
type Edge struct {
	Client         string `json:"client"`
	Server         string `json:"server"`
	ConnectionType string `json:"connection_type"`
}

// EdgeCache holds edges and provides Add and Flush methods
type EdgeCache struct {
	sync.Mutex
	interval          time.Duration
	cutoff            int64
	Edges             map[Edge]int64
	lastFlush         int64
	timeProvider      func() int64 // Function to get current time in nanoseconds
	republishInterval int64
}

// Add adds an Edge to the cache if it doesn't already exist
func (e *EdgeCache) Add(edge Edge) {
	e.Lock()
	defer e.Unlock()
	now := e.timeProvider()

	if ts, exists := e.Edges[edge]; !exists || now-ts > e.republishInterval {
		e.Edges[edge] = now
	}
}

// flush flushes new edges added since last flush
func (e *EdgeCache) flush() []Edge {
	e.Lock()
	defer e.Unlock()

	now := e.timeProvider()
	if now < e.cutoff {
		return []Edge{}
	}

	flushStart := e.timeProvider()

	var newEdges []Edge
	for edge, timestamp := range e.Edges {
		if e.lastFlush < timestamp && timestamp <= flushStart {
			newEdges = append(newEdges, edge)
		}
	}

	e.lastFlush = flushStart
	e.cutoff = e.lastFlush + e.interval.Nanoseconds()

	return newEdges
}

// NewEdgeCache initializes and returns a new EdgeCache with default time provider
func NewEdgeCache(interval time.Duration, timeProvider func() int64) *EdgeCache {
	if timeProvider == nil {
		timeProvider = func() int64 { return time.Now().UnixNano() }
	}
	now := timeProvider()
	return &EdgeCache{
		interval:          interval,
		cutoff:            now,
		Edges:             make(map[Edge]int64),
		lastFlush:         now,
		timeProvider:      timeProvider,
		republishInterval: 10 * 60 * 1e9, // 10 minutes
	}
}
