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
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type spanelement struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	ServiceName  string
	SpanName     string
	SpanKind     string
	StatusCode   string
}

func makeElements(traces ptrace.Traces) ([][]spanelement, bool, error) {
	elements := spanElements(traces)
	hasError := anyErrors(elements)
	err := junkTrace(elements)
	if err != nil {
		return nil, hasError, err
	}
	elementPaths := findPathsDFS(elements)
	return elementPaths, hasError, nil
}

func anyErrors(elements []spanelement) bool {
	for _, element := range elements {
		if element.StatusCode != "Ok" && element.StatusCode != "Unset" {
			return true
		}
	}
	return false
}

func junkTrace(elements []spanelement) error {
	parentMap := make(map[string]bool)
	hasRoot := false
	traceID := ""

	for _, element := range elements {
		parentMap[element.SpanID] = true
		if element.ParentSpanID == "" {
			if hasRoot {
				return MultipleRootsError
			}
			hasRoot = true
		}
		if traceID == "" {
			traceID = element.TraceID
		}
		if element.TraceID != traceID {
			return InconsistentTraceIDsError
		}
	}
	if !hasRoot {
		return NoRootError
	}
	for _, element := range elements {
		if element.ParentSpanID == "" {
			continue
		}
		if _, ok := parentMap[element.ParentSpanID]; !ok {
			return OrphanedSpanError
		}
	}
	return nil
}

func spanElements(traces ptrace.Traces) []spanelement {
	elements := []spanelement{}
	rss := traces.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		serviceName := "unknown"
		if serviceNameValue, found := rs.Resource().Attributes().Get("service.name"); found {
			serviceName = serviceNameValue.AsString()
		}
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				element := makeSpanElement(serviceName, span)
				elements = append(elements, element)
			}
		}
	}
	return elements
}

func makeSpanElement(serviceName string, span ptrace.Span) spanelement {
	return spanelement{
		TraceID:      span.TraceID().String(),
		SpanID:       span.SpanID().String(),
		ParentSpanID: span.ParentSpanID().String(),
		ServiceName:  serviceName,
		SpanName:     span.Name(),
		SpanKind:     span.Kind().String(),
		StatusCode:   span.Status().Code().String(),
	}
}

func findPathsDFS(elements []spanelement) [][]spanelement {
	// Create a map from parent span ID to child elements
	childrenMap := make(map[string][]spanelement)
	for _, element := range elements {
		childrenMap[element.ParentSpanID] = append(childrenMap[element.ParentSpanID], element)
	}

	// Perform a DFS from the root element
	var paths [][]spanelement
	var path []spanelement
	dfs("", childrenMap, path, &paths)
	return paths
}

func dfs(spanID string, childrenMap map[string][]spanelement, path []spanelement, paths *[][]spanelement) {
	// Copy the current path to avoid modifying it in other recursive calls
	pathCopy := append([]spanelement(nil), path...)

	// If the current span ID has no children, add the path to the paths
	if children, ok := childrenMap[spanID]; ok {
		for _, child := range children {
			dfs(child.SpanID, childrenMap, append(pathCopy, child), paths)
		}
	} else {
		*paths = append(*paths, pathCopy)
	}
}
