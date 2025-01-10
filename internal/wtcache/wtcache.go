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

package wtcache

type Cache interface {
	// Get returns the value stored in the cache for the given key.
	// If the key is not found in the cache, Get returns nil, which
	// may trigger after the cache miss processing attempts to fetch
	// data.
	Get(key string) (any, error)

	// Put stores the value in the cache for the given key and TTL.
	Put(key string, value any) error

	// Delete deletes the value in the cache for the given key.
	Delete(key string)

	// Clear clears the cache.
	Clear()

	// Dirty returns the keys for dirty items, which are defined as present in the cache
	// but the putter failed to store them.
	Dirty() []string
}
