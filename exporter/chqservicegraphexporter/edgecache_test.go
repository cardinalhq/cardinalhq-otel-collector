package chqservicegraphexporter

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
import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
)

var (
	edge1 = Edge{Client: "ClientA", Server: "ServerA", ConnectionType: "TypeA"}
	edge2 = Edge{Client: "ClientB", Server: "ServerB", ConnectionType: "TypeB"}
	edge3 = Edge{Client: "ClientC", Server: "ServerC", ConnectionType: "TypeC"}
	edge4 = Edge{Client: "ClientD", Server: "ServerD", ConnectionType: "TypeD"}
)

func TestEdgeCache_Flush_Negative(t *testing.T) {
	mockClock := clockwork.NewFakeClock()
	cache := NewEdgeCache(1*time.Minute, mockClock.Now().UnixNano)

	cache.Add(edge1)

	// Do not advance the mock clock to simulate insufficient time passage
	newEdges := cache.flush()
	if len(newEdges) != 0 {
		t.Errorf("Expected no edges, got %v", newEdges)
	}
}

func TestEdgeCache_Add_Positive(t *testing.T) {
	mockClock := clockwork.NewFakeClock()
	cache := NewEdgeCache(1*time.Minute, mockClock.Now().UnixNano)

	cache.Add(edge1)
	cache.Add(edge2)

	if len(cache.Edges) != 2 {
		t.Errorf("Expected 2 edges, got %d", len(cache.Edges))
	}
	if _, exists := cache.Edges[edge1]; !exists {
		t.Errorf("Edge1 was not added to cache")
	}
	if _, exists := cache.Edges[edge2]; !exists {
		t.Errorf("Edge2 was not added to cache")
	}
}

func TestEdgeCache_Add_Negative(t *testing.T) {
	mockClock := clockwork.NewFakeClock()
	cache := NewEdgeCache(1*time.Minute, mockClock.Now().UnixNano)

	cache.Add(edge1)
	cache.Add(edge1) // Attempt to add the same edge again

	if len(cache.Edges) != 1 {
		t.Errorf("Expected 1 unique edge, got %d", len(cache.Edges))
	}
}

func sortEdges(edges []Edge) {
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Client != edges[j].Client {
			return edges[i].Client < edges[j].Client
		}
		if edges[i].Server != edges[j].Server {
			return edges[i].Server < edges[j].Server
		}
		return edges[i].ConnectionType < edges[j].ConnectionType
	})
}

func TestEdgeCache_Flush_Multiple_Additions(t *testing.T) {
	mockClock := clockwork.NewFakeClock()
	cache := NewEdgeCache(1*time.Minute, func() int64 {
		return mockClock.Now().UnixNano()
	})

	// Simulate time passage, so that the addition time is > last flush time.
	mockClock.Advance(1 * time.Nanosecond)

	cache.Add(edge1)
	cache.Add(edge2)

	newEdges := cache.flush()
	expected := []Edge{edge1, edge2}

	sortEdges(newEdges)
	sortEdges(expected)

	if !reflect.DeepEqual(newEdges, expected) {
		t.Errorf("Expected %v, got %v", expected, newEdges)
	}

	// Simulate time passage, so that the addition time is > last flush time.
	mockClock.Advance(1 * time.Minute)

	cache.Add(edge3)
	cache.Add(edge4)

	newEdges = cache.flush()
	expected = []Edge{edge3, edge4}

	sortEdges(newEdges)
	sortEdges(expected)

	if !reflect.DeepEqual(newEdges, expected) {
		t.Errorf("Expected %v, got %v", expected, newEdges)
	}
}
