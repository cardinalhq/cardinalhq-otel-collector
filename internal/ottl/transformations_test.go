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
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"go.opentelemetry.io/collector/pdata/plog"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestIPFunctions(t *testing.T) {
	statements := []ContextStatement{
		{
			Context:    "log",
			Conditions: []string{},
			Statements: []string{
				`set(attributes["ip"], IpLocation("73.202.180.160")["city"])`,
			},
		},
	}
	instruction := Instruction{
		Statements: statements,
	}
	transformations, err := ParseTransformations(instruction, zap.NewNop())
	assert.NoError(t, err)
	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()

	tc := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
	transformations.ExecuteLogTransforms(nil, tc)
	city, cityFound := lr.Attributes().Get("ip")
	assert.True(t, cityFound)
	assert.Equal(t, "Walnut Creek", city.Str())
}

func TestIsInFunc(t *testing.T) {
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
			instruction := Instruction{
				Statements: statements,
			}
			transformations, err := ParseTransformations(instruction, zap.NewNop())
			assert.NoError(t, err)
			rm := pmetric.NewResourceMetrics()
			rm.Resource().Attributes().PutStr("service.name", tt.serviceName) // Use the test case's service name
			tc := ottlresource.NewTransformContext(rm.Resource(), rm)
			transformations.ExecuteResourceTransforms(nil, tc)
			isIn, isInFound := rm.Resource().Attributes().Get("isIn")
			assert.True(t, isInFound)
			assert.Equal(t, tt.expected, isIn.Bool()) // Use the expected result from the test case
		})
	}
}

func TestSimpleBoolean(t *testing.T) {
	statements := []ContextStatement{
		{
			Context: "log",
			Conditions: []string{
				`attributes["isTrue"] == true`,
			},
			Statements: []string{
				`set(attributes["worked"], true)`,
			},
		},
	}
	instruction := Instruction{
		Statements: statements,
	}
	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutBool("isTrue", true)

	// Parse the transformations
	transformations, err := ParseTransformations(instruction, zap.NewNop())
	if err != nil {
		t.Fatalf("Error parsing transformations: %v", err)
	}

	transformations.ExecuteLogTransforms(nil, ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl))

	attr, ok := lr.Attributes().Get("worked")
	assert.True(t, ok)
	assert.True(t, attr.Bool())
}

func TestVPCFlowLogTransformations(t *testing.T) {
	statements := []ContextStatement{
		{
			Context: "log",
			Conditions: []string{
				`attributes["logType"] == "vpcFlowFlowLogs"`,
			},
			Statements: []string{
				`replace_pattern(body, "\\s+", ",")`,
				`set(attributes["flow_log_fields"], Split(body, ","))`,
				`set(attributes["flow_version"], attributes["flow_log_fields"][0])`,
				`set(attributes["account_id"], attributes["flow_log_fields"][1])`,
				`set(attributes["interface_id"], attributes["flow_log_fields"][2])`,
				`set(attributes["source_address"], attributes["flow_log_fields"][3])`,
				`set(attributes["destination_address"], attributes["flow_log_fields"][4])`,
				`set(attributes["source_port"], attributes["flow_log_fields"][5])`,
				`set(attributes["destination_port"], attributes["flow_log_fields"][6])`,
				`set(attributes["protocol"], attributes["flow_log_fields"][7])`,
				`set(attributes["packets"], Double(attributes["flow_log_fields"][8]))`,
				`set(attributes["bytes_transferred"], Double(attributes["flow_log_fields"][9]))`,
				`set(attributes["duration"], Double(attributes["flow_log_fields"][11]) - Double(attributes["flow_log_fields"][10]))`,
				`set(attributes["action"], attributes["flow_log_fields"][12])`,
				`set(attributes["sourceLocation"], IpLocation(attributes["flow_log_fields"][3]))`,
				`set(attributes["destinationLocation"], IpLocation(attributes["flow_log_fields"][4]))`,
				`set(attributes["sourceCity"], attributes["sourceLocation"]["city"])`,
				`set(attributes["destinationCity"], attributes["destinationLocation"]["city"])`,
				`set(attributes["sourceCountry"], attributes["sourceLocation"]["country"])`,
				`set(attributes["destinationCountry"], attributes["destinationLocation"]["country"])`,
				`replace_pattern(body, ",", "\t")`,
				`delete_key(attributes, "flow_log_fields")`,
				`delete_key(attributes, "sourceLocation")`,
				`delete_key(attributes, "destinationLocation")`,
			},
		},
	}
	instruction := Instruction{
		Statements: statements,
	}
	transformations, err := ParseTransformations(instruction, zap.NewNop())
	assert.NoError(t, err)
	l := len(transformations.logTransforms)
	assert.True(t, l > 0)

	rl := plog.NewResourceLogs()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr("logType", "vpcFlowFlowLogs")
	lr.Body().SetStr("2        123456789012    eni-abc12345    10.0.0.1         10.0.1.1         443      1024     6         10       8000     1625567329   1625567389   ACCEPT   OK")
	tc := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
	transformations.ExecuteLogTransforms(nil, tc)

	version, versionFound := lr.Attributes().Get("flow_version")
	assert.True(t, versionFound)
	versionStr := version.Str()
	assert.Equal(t, "2", versionStr)

	accountId, accountIdFound := lr.Attributes().Get("account_id")
	assert.True(t, accountIdFound)
	assert.Equal(t, "123456789012", accountId.Str())

	intf, interfaceFound := lr.Attributes().Get("interface_id")
	assert.True(t, interfaceFound)
	assert.Equal(t, "eni-abc12345", intf.Str())

	// assert all the other fields were parsed correctly
	sourceAddr, sourceAddrFound := lr.Attributes().Get("source_address")
	assert.True(t, sourceAddrFound)
	assert.Equal(t, "10.0.0.1", sourceAddr.Str())

	destAddr, destAddrFound := lr.Attributes().Get("destination_address")
	assert.True(t, destAddrFound)
	assert.Equal(t, "10.0.1.1", destAddr.Str())

	srcPort, srcPortFound := lr.Attributes().Get("source_port")
	assert.True(t, srcPortFound)
	assert.Equal(t, "443", srcPort.Str())

	destPort, destPortFound := lr.Attributes().Get("destination_port")
	assert.True(t, destPortFound)
	assert.Equal(t, "1024", destPort.Str())

	protocol, protocolFound := lr.Attributes().Get("protocol")
	assert.True(t, protocolFound)
	assert.Equal(t, "6", protocol.Str())

	packets, packetsFound := lr.Attributes().Get("packets")
	assert.True(t, packetsFound)
	assert.Equal(t, 10.0, packets.Double())

	bytesTransferred, bytesTransferredFound := lr.Attributes().Get("bytes_transferred")
	assert.True(t, bytesTransferredFound)
	assert.Equal(t, 8000.0, bytesTransferred.Double())

	duration, durationFound := lr.Attributes().Get("duration")
	assert.True(t, durationFound)
	assert.Equal(t, 60.0, duration.Double())

	action, actionFound := lr.Attributes().Get("action")
	assert.True(t, actionFound)
	assert.Equal(t, "ACCEPT", action.Str())

	sourceCity, sourceCityFound := lr.Attributes().Get("sourceCity")
	assert.True(t, sourceCityFound)
	assert.Equal(t, "Unknown", sourceCity.Str())

	destinationCity, destinationCityFound := lr.Attributes().Get("destinationCity")
	assert.True(t, destinationCityFound)
	assert.Equal(t, "Unknown", destinationCity.Str())

	sourceCountry, sourceCountryFound := lr.Attributes().Get("sourceCountry")
	assert.True(t, sourceCountryFound)
	assert.Equal(t, "Unknown", sourceCountry.Str())

	destinationCountry, destinationCountryFound := lr.Attributes().Get("destinationCity")
	assert.True(t, destinationCountryFound)
	assert.Equal(t, "Unknown", destinationCountry.Str())

	expectedBody := "2\t123456789012\teni-abc12345\t10.0.0.1\t10.0.1.1\t443\t1024\t6\t10\t8000\t1625567329\t1625567389\tACCEPT\tOK"
	body := lr.Body().AsString()
	assert.Equal(t, expectedBody, body)

	_, sliceFound := lr.Attributes().Get("flow_log_fields")
	assert.False(t, sliceFound)
}

func TestTeamAssociations(t *testing.T) {
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

	instruction := Instruction{
		Statements: statements,
	}
	transformations, err := ParseTransformations(instruction, zap.NewNop())
	assert.NoError(t, err)
	l := len(transformations.resourceTransforms)
	assert.True(t, l > 0)

	rm1 := pmetric.NewResourceMetrics()
	rm1.Resource().Attributes().PutStr("service.name", "service1")
	tc := ottlresource.NewTransformContext(rm1.Resource(), rm1)
	transformations.ExecuteResourceTransforms(nil, tc)

	// check if rm1 attributes have been updated with team = "cardinal"
	team, found := rm1.Resource().Attributes().Get("team")
	assert.True(t, found)
	assert.Equal(t, "cardinal", team.Str())
}
