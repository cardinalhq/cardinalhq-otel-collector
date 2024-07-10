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

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/client"
)

type AuthEnv struct {
	customerID  string
	collectorID string
	tags        map[string]string
}

var _ translate.Environment = (*AuthEnv)(nil)

func (ae *AuthEnv) Tags() map[string]string {
	return ae.tags
}

func (ae *AuthEnv) CustomerID() string {
	return ae.customerID
}

func (ae *AuthEnv) CollectorID() string {
	return ae.collectorID
}

func EnvironmentFromAuth(ctx context.Context) translate.Environment {
	return &AuthEnv{
		customerID:  getAuthString(ctx, "client_id"),
		collectorID: getAuthString(ctx, "collector_id"),
		tags:        getAuthEnv(ctx),
	}
}

func getAuthString(ctx context.Context, key string) string {
	cl := client.FromContext(ctx)
	if cl.Auth == nil {
		return ""
	}
	v := cl.Auth.GetAttribute(key)
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func getAuthEnv(ctx context.Context) map[string]string {
	cl := client.FromContext(ctx)
	if cl.Auth == nil {
		return nil
	}
	v := cl.Auth.GetAttribute("environment")
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]string); ok {
		return m
	}
	return nil
}
