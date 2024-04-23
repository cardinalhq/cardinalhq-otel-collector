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

package fingerprinter

import (
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/db47h/ragel/v2"
)

func Fingerprint(input string) (fingerpring int64, level string) {
	l := strings.TrimSpace(input)
	l = strings.ToLower(l)
	s, level := tokenize(l)
	return int64(xxhash.Sum64String(s)), level
}

func tokenize(input string) (string, string) {
	fingerprinter := &Fingerprinter{}
	s := ragel.New("test", strings.NewReader(input), fingerprinter)
	items := []string{}
	level := ""
	for {
		_, tok, literal := s.Next()
		switch tok {
		case ragel.EOF:
			return strings.Join(items, " "), level
		case ragel.Error:
		// TODO should increment a counter here...
		case TokenLoglevel:
			level = literal
		case TokenString:
			items = append(items, literal)
		default:
			items = append(items, "<"+fingerprinter.TokenString(tok)+">")
		}
	}
}
