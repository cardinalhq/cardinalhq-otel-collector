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
		{"attribute_name", "attribute_name"},
		{"attribute-name", "attribute-name"},
		{"attribute.name", "attribute.name"},
		{"attribute name", "attribute_name"},
		{"attribute@name", "attribute_name"},
		{"_attribute_name", "attribute_name"},
		{"_____attribute_name", "attribute_name"},
		{"attribute_name___", "attribute_name"},
		{"attribute  name", "attribute_name"},
		{"Downcases", "downcases"},
		{"UPPERCASE", "uppercase"},
		{"AWSCloudWatch.max", "aws_cloud_watch.max"},
		{"aws.ApplicationELB.UnhealthyStatus", "aws.application_elb.unhealthy_status"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.Equal(t, test.expected, sanitizeAttribute(test.input))
		})
	}
}
