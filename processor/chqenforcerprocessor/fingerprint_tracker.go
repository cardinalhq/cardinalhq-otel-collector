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

package chqenforcerprocessor

import "sync"

type fingerprintTracker struct {
	sync.Mutex
	fingerprints map[uint64]struct{}
}

func newFingerprintTracker() *fingerprintTracker {
	return &fingerprintTracker{
		fingerprints: make(map[uint64]struct{}),
	}
}

func (e *chqEnforcer) newTrace(fingerprint uint64) bool {
	e.sentFingerprints.Lock()
	defer e.sentFingerprints.Unlock()
	if _, ok := e.sentFingerprints.fingerprints[fingerprint]; ok {
		return false
	}
	e.sentFingerprints.fingerprints[fingerprint] = struct{}{}
	return true
}

func (e *chqEnforcer) deleteTrace(fingerprint uint64) {
	e.sentFingerprints.Lock()
	defer e.sentFingerprints.Unlock()
	delete(e.sentFingerprints.fingerprints, fingerprint)
}
