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

package translate

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvironmentFromEnv(t *testing.T) {
	// Set up test environment variables
	os.Setenv("CARDINALHQ_CUSTOMER_ID", "12345")
	os.Setenv("CARDINALHQ_COLLECTOR_ID", "67890")
	os.Setenv("CARDINALHQ_ENV_FOO", "bar")
	os.Setenv("CARDINALHQ_ENV_BAZ", "qux")

	// Clean up environment variables after the test
	defer func() {
		os.Unsetenv("CARDINALHQ_CUSTOMER_ID")
		os.Unsetenv("CARDINALHQ_COLLECTOR_ID")
		os.Unsetenv("CARDINALHQ_ENV_FOO")
		os.Unsetenv("CARDINALHQ_ENV_BAZ")
	}()

	expected := map[string]string{
		"foo":          "bar",
		"baz":          "qux",
		"customer_id":  "12345",
		"collector_id": "67890",
	}
	env := environmentFromEnv()
	assert.NotNil(t, env)
	assert.Equal(t, expected, env.tags)
}
