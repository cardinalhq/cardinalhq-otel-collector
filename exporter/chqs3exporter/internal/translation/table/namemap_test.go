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
			"attribute.name",
			"attribute.name",
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
		{
			"UPPERCASE",
			"uppercase",
		},
		{
			"AWSCloudWatch.max",
			"aws_cloud_watch.max",
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, sanitizeAttribute(test.input))
	}
}

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			"helloWorld",
			"hello_world",
		},
		{
			"camelCase",
			"camel_case",
		},
		{
			"snake_case",
			"snake_case",
		},
		{
			"AWSCloudWatch",
			"aws_cloud_watch",
		},
		{
			"kebab-case",
			"kebab-case",
		},
		{
			"UPPER_CASE",
			"upper_case",
		},
		{
			"leadingUnderscore",
			"leading_underscore",
		},
		{
			"123Numbers456",
			"123_numbers456",
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, toSnakeCase(test.input))
	}
}
