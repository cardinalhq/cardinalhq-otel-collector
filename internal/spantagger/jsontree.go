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

type Graph struct {
	Fingerprint int64     `json:"fingerprint,omitempty"`
	Graph       *spanNode `json:"graph,omitempty"`
}

type spanNode struct {
	ServiceName string      `json:"serviceName,omitempty"`
	SpanName    string      `json:"spanName,omitempty"`
	SpanKind    string      `json:"spanKind,omitempty"`
	Children    []*spanNode `json:"children,omitempty"`
}

func BuildTree(traces ptrace.Traces, fingerprint int64) (graph *Graph, hasError bool, err error) {
	elementPaths, hasError, err := makeElements(traces)
	if err != nil {
		return nil, hasError, err
	}
	spannodes := makeTree(elementPaths)
	ret := &Graph{
		Fingerprint: fingerprint,
		Graph:       spannodes,
	}
	return ret, hasError, nil
}

func makeTree(elementPaths [][]spanelement) *spanNode {
	root := &spanNode{}
	for _, path := range elementPaths {
		if root.ServiceName == "" {
			root.ServiceName = path[0].ServiceName
			root.SpanKind = path[0].SpanKind
			root.SpanName = path[0].SpanName
		}
		current := root
		for i, element := range path {
			// Skip the first element because it is the root.
			if i == 0 {
				continue
			}
			// If the child already exists, use it as the current node.
			found := false
			for _, child := range current.Children {
				if child.ServiceName == element.ServiceName && child.SpanKind == element.SpanKind && child.SpanName == element.SpanName {
					current = child
					found = true
					break
				}
			}
			if found {
				continue
			}
			newChild := &spanNode{
				ServiceName: element.ServiceName,
				SpanKind:    element.SpanKind,
				SpanName:    element.SpanName,
			}
			current.Children = append(current.Children, newChild)
			current = newChild
		}
	}
	return root
}
