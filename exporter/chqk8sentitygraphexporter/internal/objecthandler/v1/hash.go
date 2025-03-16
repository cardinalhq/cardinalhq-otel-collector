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

package v1

import (
	"crypto/sha256"
	"math/big"
)

func calculateHashValue(header []byte, fieldname string, value []byte) string {
	hash := sha256.New()
	// We are not checking the error here because sha256 never returns an error.
	_, _ = hash.Write([]byte(header))
	_, _ = hash.Write([]byte(fieldname))
	_, _ = hash.Write(value)
	hashBytes := hash.Sum(nil)
	hashInt := new(big.Int).SetBytes(hashBytes[:])
	return hashInt.Text(10 + 26 + 26) // base = 10 digits + 26 lowercase + 26 uppercase
}
