package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeAttribute(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			"attribute_name",
			"attribute_name",
		},
		{
			"attribute-name",
			"attribute-name",
		},
		{
			"attribute name",
			"attribute_name",
		},
		{
			"attribute@name",
			"attribute_name",
		},
		{
			"_attribute_name",
			"attribute_name",
		},
		{
			"_____attribute_name",
			"attribute_name",
		},
		{
			"attribute_name___",
			"attribute_name",
		},
		{
			"attribute  name",
			"attribute_name",
		},
		{
			"Downcases",
			"downcases",
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, sanitizeAttribute(test.input))
	}
}
