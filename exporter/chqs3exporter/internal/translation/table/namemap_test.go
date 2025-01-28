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

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeAttribute(t *testing.T) {
	tests := []struct {
		input    string
		mapNames bool
		expected string
	}{
		{"attribute_name", true, "attribute_name"},
		{"attribute-name", true, "attribute_name"},
		{"attribute.name", true, "attribute.name"},
		{"attribute name", true, "attribute_name"},
		{"attribute@name", true, "attribute_name"},
		{"_attribute_name", true, "attribute_name"},
		{"_____attribute_name", true, "attribute_name"},
		{"attribute_name___", true, "attribute_name"},
		{"attribute  name", true, "attribute_name"},
		{"attribute__name", true, "attribute_name"},
		{"Downcases", true, "downcases"},
		{"UPPERCASE", true, "uppercase"},
		{"AWSCloudWatch.max", true, "aws_cloud_watch.max"},
		{"aws.ApplicationELB.UnhealthyStatus", true, "aws.application_elb.unhealthy_status"},
		{"AWS.EC2.CPUUtilization", true, "aws.ec2.cpu_utilization"},
		{"aws.k8s.cluster.cpuUtilization", true, "aws.k8s.cluster.cpu_utilization"},

		{"attribute_name", false, "attribute_name"},
		{"attribute-name", false, "attribute-name"},
		{"attribute.name", false, "attribute.name"},
		{"attribute name", false, "attribute name"},
		{"attribute@name", false, "attribute@name"},
		{"_attribute_name", false, "_attribute_name"},
		{"_____attribute_name", false, "_____attribute_name"},
		{"attribute_name___", false, "attribute_name___"},
		{"attribute  name", false, "attribute  name"},
		{"attribute__name", false, "attribute__name"},
		{"Downcases", false, "Downcases"},
		{"UPPERCASE", false, "UPPERCASE"},
		{"AWSCloudWatch.max", false, "AWSCloudWatch.max"},
		{"aws.ApplicationELB.UnhealthyStatus", false, "aws.ApplicationELB.UnhealthyStatus"},
		{"AWS.EC2.CPUUtilization", false, "AWS.EC2.CPUUtilization"},
		{"aws.k8s.cluster.cpuUtilization", false, "aws.k8s.cluster.cpuUtilization"},
	}

	for _, test := range tests {
		originalSetting := MapNames
		t.Run(test.input, func(t *testing.T) {
			MapNames = test.mapNames
			assert.Equal(t, test.expected, sanitizeAttribute(test.input))
		})
		MapNames = originalSetting
	}
}
