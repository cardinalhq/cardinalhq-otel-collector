// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

package boxer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
)

func TestSafeFilename(t *testing.T) {
	tests := []struct {
		name           string
		kind           component.Kind
		ent            component.ID
		filename       string
		expectedResult string
	}{
		{
			"empty name",
			component.KindReceiver,
			component.MustNewIDWithName("receiverType", "receiverName"),
			"",
			"receiver_receiverType_receiverName",
		},
		{
			"non-empty name",
			component.KindProcessor,
			component.MustNewIDWithName("processorType", "processorName"),
			"customName",
			"processor_processorType_processorName_customName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SafeFilename(tt.kind, tt.ent, tt.filename)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestSanitize(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedOutput string
	}{
		{
			"no unsafe characters",
			"safeName",
			"safeName",
		},
		{
			"unsafe characters",
			"unsafe/Name~",
			"unsafe~002FName~007E",
		},
		{
			"mixed safe and unsafe characters",
			"mixed.Name/with~Unsafe",
			"mixed.Name~002Fwith~007EUnsafe",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitize(tt.input)
			assert.Equal(t, tt.expectedOutput, result)
		})
	}
}

func TestKindString(t *testing.T) {
	tests := []struct {
		name     string
		kind     component.Kind
		expected string
	}{
		{
			"receiver",
			component.KindReceiver,
			"receiver",
		},
		{
			"processor",
			component.KindProcessor,
			"processor",
		},
		{
			"exporter",
			component.KindExporter,
			"exporter",
		},
		{
			"extension",
			component.KindExtension,
			"extension",
		},
		{
			"connector",
			component.KindConnector,
			"connector",
		},
		{
			"other",
			component.Kind{}, // arbitrary value for testing unexpected kind
			"other",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := kindString(tt.kind)
			assert.Equal(t, tt.expected, result)
		})
	}
}
