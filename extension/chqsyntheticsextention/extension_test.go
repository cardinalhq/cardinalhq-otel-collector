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

package chqsyntheticsextention

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestPollTarget_Success(t *testing.T) {
	extension := &CHQSyntheticsExtension{
		logger: zap.NewNop(),
	}

	wasCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/checkmate", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"value":"checkmate!"}`))
		wasCalled = true
	}))
	defer server.Close()

	target := ottl.SyntheticPollingTarget{
		Endpoint: server.URL + "/checkmate",
		Method:   "GET",
		Timeout:  5 * time.Second,
	}

	extension.pollTarget("tenant1", http.DefaultClient, target)
	assert.True(t, wasCalled)
}
