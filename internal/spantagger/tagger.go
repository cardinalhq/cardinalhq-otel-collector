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

package spantagger

import (
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Fingerprint generates a fingerprint for a set of traces. The fingerprint is a string that
// represents the sorted, unique paths in the traces.
//
// If an error is returned, it will be of type Error and will indicate the problem with the
// trace.  The fingerprint will be 0 in this case.
func Fingerprint(traces ptrace.Traces) (fingerprint uint64, hasError bool, err error) {
	elementPaths, hasError, err := makeElements(traces)
	if err != nil {
		return 0, hasError, err
	}
	pathstrings := spanPathStrings(elementPaths)
	uniquePathstrings := uniquePathStrings(pathstrings)
	fingerprint = xxhash.Sum64String(strings.Join(uniquePathstrings, "##"))

	return fingerprint, hasError, nil
}

func elementPathString(element spanelement) string {
	return strings.Join([]string{element.ServiceName, element.SpanName, element.SpanKind}, ":")
}

func spanPathStrings(elementPaths [][]spanelement) []string {
	pathstrings := []string{}
	for _, path := range elementPaths {
		pathnames := []string{}
		for _, element := range path {
			pathnames = append(pathnames, elementPathString(element))
		}
		pathstrings = append(pathstrings, strings.Join(pathnames, "::"))
	}
	return pathstrings
}

func uniquePathStrings(pathstrings []string) []string {
	slices.Sort(pathstrings)
	ret := []string{}
	last := ""
	for _, path := range pathstrings {
		if path != last {
			ret = append(ret, path)
			last = path
		}
	}
	return ret
}
