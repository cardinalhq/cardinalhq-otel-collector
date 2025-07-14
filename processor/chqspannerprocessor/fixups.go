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

package chqspannerprocessor

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func serviceNameFromAddress(addr string) string {
	if addr == "" || addr == "localhost" || addr == "127.0.0.1" {
		return ""
	}

	// convert prod-us-east-2-global.cluster-zzz.us-east-2.rds.amazonaws.com -> prod-us-east-2-global
	// convert prod-us-east-2-global-#.zzz.us-east-2.rds.amazonaws.com -> prod-us-east-2-global
	if !strings.HasSuffix(addr, ".rds.amazonaws.com") {
		return ""
	}
	parts := strings.Split(addr, ".")
	if len(parts) != 6 {
		return ""
	}
	serviceName := parts[0]
	if !strings.HasPrefix(parts[1], "cluster-") {
		if idx := strings.LastIndex(serviceName, "-"); idx != -1 {
			serviceName = serviceName[:idx]
		}
	}
	return serviceName
}

func serviceNameFromServerAddress(span ptrace.Span) string {
	serverAddress, found := span.Attributes().Get("server.address")
	if !found {
		return ""
	}
	saddr := serverAddress.AsString()
	return serviceNameFromAddress(saddr)
}
