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

import "time"

// KVS is a key-value store that uses []byte as keys and values.
// Values have an optional TTL.  Using a TTL of 0 means the value never expires.
// Implementations must be concurrent-safe.
type KVS interface {
	// Get retrieves the value for the given key.
	// If the key does not exist, it returns nil value
	Get(key []byte) (value []byte, err error)

	// Set sets the value for the given key.
	Set(key []byte, value []byte, ttl time.Duration) error

	// Delete deletes the value for the given key.
	// If the key does not exist, it does nothing.
	Delete(key ...[]byte) error

	// ForEachPrefix calls the given function for each key-value pair with the given prefix.
	// If the function returns false, the iteration stops.
	// It is safe to delete the keys returned by this function.
	ForEachPrefix(prefix []byte, f func(key []byte, value []byte) bool) error

	// Maintain the key-value store.  This may call GC or other maintenance tasks.
	Maintain() error

	// Close closes the key-value store.
	Close() error
}

// Wiper interface is optional, and used mostly for testing,
// using the in-memory KVS.  This method may not be concurrent-safe,
// and should not be used in production.
type Wiper interface {
	// Wipe removes all keys from the key-value store.
	Wipe() error
}
