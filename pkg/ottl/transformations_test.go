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

package ottl

import (
	"strings"
	"testing"

	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/translate"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestIPFunctions(t *testing.T) {
	logger := zap.NewNop()
	statements := []ContextStatement{
		{
			Context:    "log",
			Conditions: []string{},
			Statements: []string{
				`set(attributes["ip"], IpLocation("73.202.180.160")["city"])`,
			},
		},
	}
	transformations, err := ParseTransformations(logger, statements)
	assert.NoError(t, err)
	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()

	tc := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
	transformations.ExecuteLogTransforms(logger, nil, nil, tc)
	city, cityFound := lr.Attributes().Get("ip")
	assert.True(t, cityFound)
	assert.Equal(t, "Walnut Creek", city.Str())
}

func TestIsInFunc(t *testing.T) {
	logger := zap.NewNop()
	tests := []struct {
		name        string
		serviceName string
		expected    bool
	}{
		{
			name:        "Positive case: Service in the list",
			serviceName: "service1",
			expected:    true,
		},
		{
			name:        "Negative case: Service not in the list",
			serviceName: "service4",
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements := []ContextStatement{
				{
					Context:    "resource",
					Conditions: []string{},
					Statements: []string{
						`set(attributes["isIn"], IsIn(attributes["service.name"], ["service1", "service2", "service3"]))`,
					},
				},
			}
			transformations, err := ParseTransformations(logger, statements)
			assert.NoError(t, err)
			rm := pmetric.NewResourceMetrics()
			rm.Resource().Attributes().PutStr("service.name", tt.serviceName) // Use the test case's service name
			tc := ottlresource.NewTransformContext(rm.Resource(), rm)
			transformations.ExecuteResourceTransforms(logger, nil, nil, tc)
			isIn, isInFound := rm.Resource().Attributes().Get("isIn")
			assert.True(t, isInFound)
			assert.Equal(t, tt.expected, isIn.Bool()) // Use the expected result from the test case
		})
	}
}

func TestSimpleBoolean(t *testing.T) {
	logger := zap.NewNop()
	statements := []ContextStatement{
		{
			Context: "log",
			Conditions: []string{
				`Exists(attributes["isTrue"]) and attributes["isTrue"] == true`,
			},
			Statements: []string{
				`set(attributes["worked"], true)`,
			},
		},
	}
	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutBool("isTrue", true)

	// Parse the transformations
	transformations, err := ParseTransformations(logger, statements)
	if err != nil {
		t.Fatalf("Error parsing transformations: %v", err)
	}

	transformations.ExecuteLogTransforms(logger, nil, nil, ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl))

	attr, ok := lr.Attributes().Get("worked")
	assert.True(t, ok)
	assert.True(t, attr.Bool())
}

func TestSeverity(t *testing.T) {
	logger := zap.NewNop()
	rl := plog.NewResourceLogs()
	rl.Resource().Attributes().PutStr(translate.CardinalFieldReceiverType, "awsfirehose")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetSeverityText("INFO")

	statements := []ContextStatement{
		{
			Context: "log",
			Conditions: []string{
				`severity_text == "INFO"`,
			},
			Statements: []string{
				`set(attributes["foo"], "INFO")`,
			},
		},
	}
	transformations, err := ParseTransformations(logger, statements)
	assert.NoError(t, err)
	assert.True(t, len(transformations.logTransforms) > 0)
	transformations.ExecuteLogTransforms(logger, nil, nil, ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl))

	// assert if foo exists and is set to INFO
	foo, fooFound := lr.Attributes().Get("foo")
	assert.True(t, fooFound)
	assert.Equal(t, "INFO", foo.Str())
}

func TestAccessLogs_UsingGrok(t *testing.T) {
	logger := zap.NewNop()
	statements1 := []ContextStatement{
		{
			Context:    "log",
			Conditions: []string{},
			Statements: []string{
				`set(attributes["fields"], DeriveSourceType(body, resource.attributes["_cardinalhq.receiver_type"]))`,
			},
		},
	}
	transformations1, err := ParseTransformations(logger, statements1)
	assert.NoError(t, err)
	assert.True(t, len(transformations1.logTransforms) > 0)

	rl := plog.NewResourceLogs()
	rl.Resource().Attributes().PutStr(translate.CardinalFieldReceiverType, "datadog")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("10.1.1.140 - - [16/May/2022:15:01:52 -0700] \"GET /themes/ComBeta/images/bullet.png HTTP/1.1\" 404 304")
	tc := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
	transformations1.ExecuteLogTransforms(logger, nil, nil, tc)

	fields, fieldsFound := lr.Attributes().Get("fields")
	assert.True(t, fieldsFound)
	m1 := fields.Map().AsRaw()
	assert.Equal(t, 10, len(m1))
	assert.True(t, m1["sourceType"] == "accessLogs")

	statements2 := []ContextStatement{
		{
			Context: "log",
			Conditions: []string{
				`attributes["fields"]["sourceType"] == "accessLogs" and Int(attributes["fields"]["response_code"]) >= 400`,
			},
			Statements: []string{
				`set(attributes["isTrue"], true)`,
			},
		},
	}

	transformations2, err := ParseTransformations(logger, statements2)
	assert.NoError(t, err)
	assert.True(t, len(transformations2.logTransforms) > 0)

	transformations2.ExecuteLogTransforms(logger, nil, nil, tc)
	get, b := lr.Attributes().Get("isTrue")
	assert.True(t, b)
	assert.True(t, get.Bool())
}

func TestVPCFlowLogTransformation_UsingGrok(t *testing.T) {
	logger := zap.NewNop()
	statements1 := []ContextStatement{
		{
			Context:    "log",
			Conditions: []string{},
			Statements: []string{
				`set(attributes["fields"], DeriveSourceType(body, resource.attributes["_cardinalhq.receiver_type"]))`,
			},
		},
	}
	transformations1, err := ParseTransformations(logger, statements1)
	assert.NoError(t, err)
	assert.True(t, len(transformations1.logTransforms) > 0)

	rl := plog.NewResourceLogs()
	rl.Resource().Attributes().PutStr(translate.CardinalFieldReceiverType, "awsfirehose")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("2        123456789012    eni-abc12345    10.0.0.1         10.0.1.1         443      1024     6         10       8000     1625567329   1625567389   ACCEPT   OK")
	tc := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
	transformations1.ExecuteLogTransforms(logger, nil, nil, tc)

	fields, fieldsFound := lr.Attributes().Get("fields")
	assert.True(t, fieldsFound)
	m1 := fields.Map().AsRaw()
	assert.Equal(t, 15, len(m1))

	statements2 := []ContextStatement{
		{
			Context: "log",
			Conditions: []string{
				`attributes["fields"]["sourceType"] == "vpcFlowLogs"`,
			},
			Statements: []string{
				`set(attributes["fields"]["duration"], Double(attributes["fields"]["endTime"]) - Double(attributes["fields"]["startTime"]))`,
				`set(attributes["fields"]["sourceLocation"], IpLocation(attributes["fields"]["sourceIp"]))`,
				`set(attributes["fields"]["destinationLocation"], IpLocation(attributes["fields"]["destinationIp"]))`,
				`set(attributes["fields"]["sourceCity"], attributes["fields"]["sourceLocation"]["city"])`,
				`set(attributes["fields"]["destinationCity"], attributes["fields"]["destinationLocation"]["city"])`,
				`set(attributes["fields"]["sourceCountry"], attributes["fields"]["sourceLocation"]["country"])`,
				`set(attributes["fields"]["destinationCountry"], attributes["fields"]["destinationLocation"]["country"])`,
			},
		},
	}
	transformations2, err := ParseTransformations(logger, statements2)
	assert.NoError(t, err)
	assert.True(t, len(transformations2.logTransforms) > 0)
	transformations2.ExecuteLogTransforms(logger, nil, nil, tc)

	fields2, _ := lr.Attributes().Get("fields")
	m2 := fields2.Map().AsRaw()
	duration, durationFound := m2["duration"]
	assert.True(t, durationFound)
	assert.Equal(t, 60.0, duration)
}

func TestAccessLogs_UsingLookup(t *testing.T) {
	logger := zap.NewNop()

	// `set(attributes["isIn"], IsIn(attributes["service.name"], ["service1", "service2", "service3"]))`,

	statements := []ContextStatement{
		{
			Context:    "log",
			Conditions: []string{},
			Statements: []string{
				`set(attributes["fields"]["method_code"], Int(Lookup(attributes["method"], ["GET", "3", "CONNECT", "4"], "99")))`,
			},
		},
	}

	transformations, err := ParseTransformations(logger, statements)
	assert.NoError(t, err)
	assert.True(t, len(transformations.logTransforms) > 0)

	rl := plog.NewResourceLogs()
	rl.Resource().Attributes().PutStr(translate.CardinalFieldReceiverType, "datadog")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr("method", "GET")

	tc := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
	transformations.ExecuteLogTransforms(logger, nil, nil, tc)

	fields, fieldsFound := lr.Attributes().Get("fields")
	assert.True(t, fieldsFound)

	methodCode, methodCodeFound := fields.Map().Get("method_code")
	assert.True(t, methodCodeFound)
	assert.Equal(t, methodCode.Int(), int64(3))
}

func TestLogSeverityRule(t *testing.T) {
	logger := zap.NewNop()
	statements := []ContextStatement{
		{
			Context: "log",
			Conditions: []string{
				`severity_text == "INFO"`,
			},
			Statements: []string{
				`set(attributes["foo"], "INFO")`,
			},
		},
	}
	transformations, err := ParseTransformations(logger, statements)
	assert.NoError(t, err)
	assert.True(t, len(transformations.logTransforms) > 0)

	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetSeverityText("INFO")

	transformations.ExecuteLogTransforms(logger, nil, nil, ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl))

	foo, fooFound := lr.Attributes().Get("foo")
	assert.True(t, fooFound)
	assert.Equal(t, "INFO", foo.Str())
}

func TestPIIRegexRules(t *testing.T) {
	logger := zap.NewNop()
	statements := []ContextStatement{
		{
			Context:    "log",
			Conditions: []string{},
			Statements: []string{
				`replace_pattern(body, "([0-9A-Fa-f]{2}[:\\-]){5}([0-9A-Fa-f]{2})", "<mac_address>")`,
				`replace_pattern(body, "[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}", "<bank_account_number>")`,
				`replace_pattern(body, "([0-9]{4}[- ]?){3}[0-9]{2,4}", "<cc>")`,
				`replace_pattern(body, "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}", "<email>")`,
				`replace_pattern(body, "(?:[0-9]{1,3}\\.){3}[0-9]{1,3}", "<ipv4>")`,
				`replace_pattern(body, "([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}", "<ipv6>")`,
				`replace_pattern(body, "\\d{1,5}\\s\\w+(\\s\\w+)*\\s(?:Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|Lane|Ln|Court|Ct|Circle|Cir|Way|Plaza|Pl|Place|Sq|Square|Loop)\\s?(?:Apt|Apartment|Suite|Ste|#)?\\s?\\d{0,5},?\\s\\w+(\\s\\w+)*,\\s[A-Z]{2}\\s\\d{5}(-\\d{4})?", "<street_address>")`,
				`replace_pattern(body, "[2-9][0-9]{2}\\)?[-.\\s]?[0-9]{3}[-.\\s]?[0-9]{4}", "<phone_number>")`,
				`replace_all_patterns(attributes, "value", "[2-9][0-9]{2}\\)?[-.\\s]?[0-9]{3}[-.\\s]?[0-9]{4}", "<phone_number>")`,
			},
		},
	}

	transformations, err := ParseTransformations(logger, statements)
	assert.NoError(t, err)
	assert.True(t, len(transformations.logTransforms) > 0)

	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("This is a credit card number: 5610591081018250\n" +
		"This is a US Address: 1234 Main St, Walnut Creek, CA 94596\n" +
		"This is a phone number: 925-555-1212\n" +
		"This is a IBAN number: DE89370400440532013000\n" +
		"This is a IPV4 address: 192.168.1.1\n" +
		"This is a IPV6 address: 2001:0db8:85a3:0000:0000:8a2e:0370:7334\n" +
		"This is a email address: foo.bar@gmail.com\n" +
		"This is a MAC Address: 00:1A:2B:3C:4D:5E\n" +
		"This is a SSN: 200-10-1234\n")
	lr.Attributes().PutStr("message", "This is a phone number: 925-555-1212")

	tc := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
	transformations.ExecuteLogTransforms(logger, nil, nil, tc)

	// check if body has been updated with cc
	body := lr.Body().Str()
	assert.True(t, strings.Contains(body, "cc"))
	assert.False(t, strings.Contains(body, "5610591081018250"))

	assert.True(t, strings.Contains(body, "email"))
	assert.False(t, strings.Contains(body, "foo.bar@gmail.com"))

	assert.True(t, strings.Contains(body, "ipv4"))
	assert.False(t, strings.Contains(body, "192.168.1.1"))

	assert.True(t, strings.Contains(body, "ipv6"))
	assert.False(t, strings.Contains(body, "2001:0db8:85a3:0000:0000:8a2e:0370:7334"))

	assert.True(t, strings.Contains(body, "street_address"))
	assert.False(t, strings.Contains(body, "1234 Main St, Walnut Creek, CA 94596"))

	assert.True(t, strings.Contains(body, "phone_number"))
	assert.False(t, strings.Contains(body, "925-555-1212"))

	assert.True(t, strings.Contains(body, "bank_account_number"))
	assert.False(t, strings.Contains(body, "DE89370400440532013000"))

	assert.True(t, strings.Contains(body, "mac_address"))
	assert.False(t, strings.Contains(body, "00:1A:2B:3C:4D:5E"))

	message, messageFound := lr.Attributes().Get("message")
	assert.True(t, messageFound)
	assert.Equal(t, "This is a phone number: <phone_number>", message.Str())

}

func TestTeamAssociations(t *testing.T) {
	logger := zap.NewNop()
	statements := []ContextStatement{
		{
			Context: "resource",
			Conditions: []string{
				`IsMatch(attributes["service.name"], "service1|service2|service3")`,
			},
			Statements: []string{
				`set(attributes["team"], "cardinal")`,
			},
		},
	}

	transformations, err := ParseTransformations(logger, statements)
	assert.NoError(t, err)
	l := len(transformations.resourceTransforms)
	assert.True(t, l > 0)

	rm1 := pmetric.NewResourceMetrics()
	rm1.Resource().Attributes().PutStr("service.name", "service1")
	tc := ottlresource.NewTransformContext(rm1.Resource(), rm1)
	transformations.ExecuteResourceTransforms(logger, nil, nil, tc)

	// check if rm1 attributes have been updated with team = "cardinal"
	team, found := rm1.Resource().Attributes().Get("team")
	assert.True(t, found)
	assert.Equal(t, "cardinal", team.Str())
}
