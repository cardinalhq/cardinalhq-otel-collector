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

package fingerprinter

import (
	"bufio"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFingerprinterWithKafkaBroker0(t *testing.T) {
	file, err := os.Open("testdata/kafka-broker-0.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	fp := NewFingerprinter()
	for scanner.Scan() {
		input := scanner.Text()
		t.Run(input, func(t *testing.T) {
			_, _, err := fp.Tokenize(strings.ToLower(input))
			assert.NoError(t, err)
		})
	}
}

func TestFingerprinter(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      string
		wantLevel string
	}{
		{
			"empty",
			"",
			"",
			"",
		},
		{
			"simple",
			"hello world",
			"hello world",
			"",
		},
		{
			"date YYYY-MM-DD",
			"2024-01-02",
			"<Date>",
			"",
		},
		{
			"date YYYY/MM/DD",
			"2024/01/02",
			"<Date>",
			"",
		},
		{
			"date DD/MM/YY",
			"02/01/24",
			"<Date>",
			"",
		},
		{
			"time",
			"14:54:12",
			"<Time>",
			"",
		},
		{
			"uuid",
			"dddddddd-dddd-dddd-dddd-dddddddddddd",
			"<UUID>",
			"",
		},
		{
			"ipv4",
			"10.42.255.254",
			"<IPv4>",
			"",
		},
		{
			"simple email address",
			"alice@example.com",
			"<Email>",
			"",
		},
		{
			"email with _",
			"alice_smith@example.com",
			"<Email>",
			"",
		},
		{
			"email with -",
			"alice-smith@example.com",
			"<Email>",
			"",
		},
		{
			"email with +",
			"alice+smith@example.com",
			"<Email>",
			"",
		},
		{
			"email with .",
			"alice.smith@example.com",
			"<Email>",
			"",
		},
		{
			"example.com",
			"example.com",
			"<FQDN>",
			"",
		},
		{
			"path alone",
			" /api/v10/endpoint",
			"<Path>",
			"",
		},
		{
			"path with version",
			"bob /api/v10/endpoint",
			"bob <Path>",
			"",
		},
		{
			"sample log 1",
			`2024-04-17 00:37:23.147 ERROR 1 --- [lt-dispatcher-5] c.g.d.TelemetryEmitter : Received error code 400, endpoint = /api/v10/endpoint`,
			"<Date> <Time> <Loglevel> <Number> <Identifier> <FQDN> received error code <Number> endpoint <Path>",
			"error",
		},
		{
			"sample log 2",
			`	advertised.listeners = CLIENT://kafka-kraft-broker-0.kafka-kraft-broker-headless.default.svc.cluster.local:9092,INTERNAL://kafka-kraft-broker-0.kafka-kraft-broker-headless.default.svc.cluster.local:9094
`,
			"<FQDN> <Url> <Url>",
			"",
		},
		{
			"sample log 3",
			`   foo = CLIENT://:1234,INTERNAL://:5678`,
			"foo <Url> <Url>",
			"",
		},
		{
			"sample log 4",
			`Receive ListRecommendations for product ids:['OLJCESPC7Z', '6E92ZMYYFZ', '1YMWWN1N4O', 'L9ECAV7KIM', '2ZYFJ3GM2N']`,
			"receive listrecommendations for product ids",
			"",
		},
	}
	for _, tt := range tests {
		fp := NewFingerprinter()
		t.Run(tt.name, func(t *testing.T) {
			tokens, level, err := fp.Tokenize(tt.input)
			assert.NoError(t, err, "input: %s", tt.input)
			assert.Equal(t, tt.want, tokens, "input: %s", tt.input)
			assert.Equal(t, tt.wantLevel, level, "input: %s", tt.input)
		})
	}
}

func BenchmarkFingerprinter1(b *testing.B) {
	input := "[2024-04-06 21:23:32,742] INFO [GroupCoordinator 100]: Preparing to rebalance group metadata.ingest.stats.consumer in state PreparingRebalance with old generation 14 (__consumer_offsets-14) (reason: Adding new member metadata.ingest.stats.consumer-0-e78065b6-0f83-4397-92ae-965997f4b1a2 with group instance id Some(metadata.ingest.stats.consumer-0); client reason: not provided) (kafka.coordinator.group.GroupCoordinator)"
	fp := NewFingerprinter()
	log.Printf("Running loop for %d times", b.N)
	for i := 0; i < b.N; i++ {
		_, _, err := fp.Fingerprint(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSplitWords(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			"empty",
			"",
			nil,
		},
		{
			"snake_case",
			"hello_world",
			[]string{"hello", "world"},
		},
		{
			"camelCase",
			"helloWorld",
			[]string{"hello", "world"},
		},
		{
			"CamelCase",
			"HelloWorld",
			[]string{"hello", "world"},
		},
		{
			"longer_snake_case",
			"hello_world_this_is_a_test",
			[]string{"hello", "world", "this", "is", "a", "test"},
		},
		{
			"longer_camelCase",
			"helloWorldThisIsATest",
			[]string{"hello", "world", "this", "is", "a", "test"},
		},
		{
			"longer_CamelCase",
			"HelloWorldThisIsATest",
			[]string{"hello", "world", "this", "is", "a", "test"},
		},
		{
			"THISIsATest",
			"THISIsATest",
			[]string{"t", "h", "i", "s", "is", "a", "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, splitWords(tt.input))
		})
	}
}
