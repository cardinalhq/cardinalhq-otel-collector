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

package badgerbox

//
// The package badgerbox provides a simple interface for storing and retrieving
// data in a key-value store.  It is designed to be used with BadgerDB, but can
// be used with any key-value store that implements the KVS interface.
//
// The Box type is the primary type in this package.  It is used to store and
// retrieve data from the key-value store.  The Box type is created with the
// NewBox function, which takes a KVS, an interval, an interval count, a grace
// period, a time-to-live (TTL) duration, and a TimeFunc.  The KVS is the key-
// value store to use, the interval is the time interval to use for storing data,
// the interval count is the number of intervals to keep, the grace period is
// the amount of time to wait after a timebox has closed before it is available
// for reading, the TTL is the time-to-live duration for the data, and the
// TimeFunc is a function that returns the current time.  The TimeFunc will
// generally be `nil` and will default to `time.Now` if so.
//
// The Box type has several methods for storing and retrieving data.  The
// Put method stores data in the key-value store.  The Get method
// retrieves data from the key-value store.  The Delete method deletes data from
// the key-value store.  The Close method closes the Box and releases any
// resources it is using.
//
// The keys in the KVS are of the format `interval-scope-random`, where interval
// is the interval number, scope is the scope of the data, and random is a random
// number between 100000000000 and 999999999999.  The interval number is
// the current time divided by the interval duration.  The scope is a string
// that can be used for anything desired, like a customer ID or a sensor ID.
// Its purpose is to give the effect of multiple timeboxes in the same interval,
// but with different content or different dispositions of the data.
//
// After a given time interval has passed, the Box will stop allowing writes to
// that interval and will start allowing reads.
//
// The Box will keep the last `intervalCount` intervals in the key-value store
// and available for writing.
//
// Getting the current oldest interval can be done with the `OldestInterval`
// method.  This will return a timestamp for the oldest interval currently in
// the key-value store that is closed and available for reading.
//
// Records must be deleted from the key-value store after they are no longer
// needed.  The Box will not automatically delete records, and it can be done
// with the `Delete` method while iterating.
//
// Periodically, the `Maintain` method should be called to clean up old records
// and to perform other database maintenance tasks.  This should not be called
// every interval, but rather every 5 minutes or so.  Depending on the implementation
// of the KVS, this may block reads and writes for a short time.
//
