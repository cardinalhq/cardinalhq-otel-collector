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

package chqs3exporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/client"
)

type testAuth struct{}

func (ta *testAuth) GetAttribute(key string) any {
	switch key {
	case "string":
		return "abc123"
	case "int":
		return 123
	case "bool":
		return true
	case "client_id":
		return "client1"
	case "collector_id":
		return "collector1"
	case "environment":
		return map[string]string{
			"bob":      "bobby",
			"samantha": "sam",
			"joseph":   "joe",
		}
	default:
		return nil
	}
}

func (ta *testAuth) GetAttributeNames() []string {
	return []string{"string", "int", "bool", "client_id", "collector_id", "environment"}
}

var _ client.AuthData = (*testAuth)(nil)

func TestGetAuthString(t *testing.T) {
	ctx := context.Background()
	ci := client.Info{
		Auth: &testAuth{},
	}
	ctx = client.NewContext(ctx, ci)

	tests := []struct {
		key    string
		result string
	}{
		{
			key:    "string",
			result: "abc123",
		},
		{
			key:    "int",
			result: "",
		},
		{
			key:    "bool",
			result: "",
		},
		{
			key:    "unknown",
			result: "",
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.result, getAuthString(ctx, test.key))
	}
}

func TestGetAuthString_noauth(t *testing.T) {
	ctx := context.Background()
	ci := client.Info{
		Auth: nil,
	}
	ctx = client.NewContext(ctx, ci)

	assert.Equal(t, "", getAuthString(ctx, "string"))
}

func TestGetAuthEnv(t *testing.T) {
	ctx := context.Background()
	ci := client.Info{
		Auth: &testAuth{},
	}
	ctx = client.NewContext(ctx, ci)

	assert.Equal(t, map[string]string{
		"bob":      "bobby",
		"samantha": "sam",
		"joseph":   "joe",
	}, getAuthEnv(ctx))
}

func TestGetAuthEnv_noauth(t *testing.T) {
	ctx := context.Background()
	ci := client.Info{
		Auth: nil,
	}
	ctx = client.NewContext(ctx, ci)

	assert.Nil(t, getAuthEnv(ctx))
}

func TestEnvironmentFromAuth(t *testing.T) {
	ctx := context.Background()
	ci := client.Info{
		Auth: &testAuth{},
	}
	ctx = client.NewContext(ctx, ci)

	ae := EnvironmentFromAuth(ctx)
	assert.Equal(t, "client1", ae.CustomerID())
	assert.Equal(t, "collector1", ae.CollectorID())
	assert.Equal(t, map[string]string{
		"bob":      "bobby",
		"samantha": "sam",
		"joseph":   "joe",
	}, ae.Tags())
}
