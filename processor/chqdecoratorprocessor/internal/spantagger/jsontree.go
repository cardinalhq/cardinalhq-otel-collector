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

package spantagger

import (
	"encoding/json"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type spanNode struct {
	ServiceName string     `json:"serviceName,omitempty" yaml:"serviceName,omitempty"`
	SpanName    string     `json:"spanName,omitempty" yaml:"spanName,omitempty"`
	SpanKind    string     `json:"spanKind,omitempty" yaml:"spanKind,omitempty"`
	Children    []spanNode `json:"children,omitempty" yaml:"children,omitempty"`
}

func TreeToJSON(root spanNode) string {
	b, err := json.Marshal(root)
	if err != nil {
		return ""
	}
	return string(b)
}

func BuildTree(traces ptrace.Traces) (root spanNode, hasError bool, err error) {
	elementPaths, hasError, err := makeElements(traces)
	if err != nil {
		return spanNode{}, hasError, err
	}
	return makeTree(elementPaths), hasError, nil
}

func makeTree(elementPaths [][]spanelement) spanNode {
	root := spanNode{}
	for _, path := range elementPaths {
		if root.ServiceName == "" {
			root.ServiceName = path[0].ServiceName
			root.SpanKind = path[0].SpanKind
			root.SpanName = path[0].SpanName
		}
		current := &root
		for i, element := range path {
			if i == 0 {
				continue
			}
			newChild := spanNode{
				ServiceName: element.ServiceName,
				SpanKind:    element.SpanKind,
				SpanName:    element.SpanName,
			}
			current.Children = append(current.Children, newChild)
			current = &current.Children[len(current.Children)-1]
		}
	}
	return root
}
