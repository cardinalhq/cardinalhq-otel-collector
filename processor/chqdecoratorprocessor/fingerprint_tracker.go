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

package chqdecoratorprocessor

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

func (sp *spansProcessor) newTrace(fingerprint uint64) bool {
	sp.sentFingerprints.Lock()
	defer sp.sentFingerprints.Unlock()
	if _, ok := sp.sentFingerprints.fingerprints[fingerprint]; ok {
		return false
	}
	sp.sentFingerprints.fingerprints[fingerprint] = struct{}{}
	return true
}

func (sp *spansProcessor) deleteTrace(fingerprint uint64) {
	sp.sentFingerprints.Lock()
	defer sp.sentFingerprints.Unlock()
	delete(sp.sentFingerprints.fingerprints, fingerprint)
}
