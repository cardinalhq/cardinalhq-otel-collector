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
