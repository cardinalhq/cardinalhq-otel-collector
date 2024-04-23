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

	"github.com/cespare/xxhash/v2"
	"github.com/db47h/ragel/v2"
	"github.com/stretchr/testify/assert"
)

func TestFingerprinterWithKafkaBroker0(t *testing.T) {
	file, err := os.Open("testdata/kafka-broker-0.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		input := scanner.Text()
		fp := Fingerprinter{}
		s := ragel.New("test", strings.NewReader(strings.ToLower(input)), &fp)
		_ = consumet(t, s, &fp)
	}
}

func consumet(t *testing.T, s *ragel.Scanner, fingerprinter *Fingerprinter) string {
	items := []string{}
	for {
		pos, tok, literal := s.Next()
		switch tok {
		case ragel.EOF:
			return strings.Join(items, " ")
		case ragel.Error:
			t.Fatalf("scan error: %v: %v\n", s.Pos(pos), literal)
		case TokenString:
			items = append(items, literal)
		default:
			items = append(items, "<"+fingerprinter.TokenString(tok)+">")
		}
	}
}

func TestFingerprinter(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty",
			input: "",
			want:  "",
		},
		{
			name:  "simple",
			input: "hello world",
			want:  "hello world",
		},
		{
			name:  "date YYYY-MM-DD",
			input: "2024-01-02",
			want:  "<Date>",
		},
		{
			name:  "date YYYY/MM/DD",
			input: "2024/01/02",
			want:  "<Date>",
		},
		{
			name:  "date DD/MM/YY",
			input: "02/01/24",
			want:  "<Date>",
		},
		{
			name:  "time",
			input: "14:54:12",
			want:  "<Time>",
		},
		{
			name:  "uuid",
			input: "dddddddd-dddd-dddd-dddd-dddddddddddd",
			want:  "<UUID>",
		},
		{
			name:  "ipv4",
			input: "10.42.255.254",
			want:  "<IPv4>",
		},
		{
			name:  "simple email address",
			input: "alice@example.com",
			want:  "<Email>",
		},
		{
			name:  "email with _",
			input: "alice_smith@example.com",
			want:  "<Email>",
		},
		{
			name:  "email with -",
			input: "alice-smith@example.com",
			want:  "<Email>",
		},
		{
			name:  "email with +",
			input: "alice+smith@example.com",
			want:  "<Email>",
		},
		{
			name:  "email with .",
			input: "alice.smith@example.com",
			want:  "<Email>",
		},
		{
			name:  "example.com",
			input: "example.com",
			want:  "<FQDN>",
		},
		{
			name:  "path with version",
			input: "bob /api/v10/endpoint",
			// should be "bob <Path>"
			want: "bob api <Path>",
		},
		{
			name:  "sample log 1",
			input: `2024-04-17 00:37:23.147 ERROR 1 --- [lt-dispatcher-5] c.g.d.TelemetryEmitter : Received error code 400, endpoint = /api/v10/endpoint`,
			// should be ... endpoint <Path>
			want: "<Date> <Time> <Loglevel> <Number> <Identifier> <FQDN> received <Loglevel> code <Number> endpoint api <Path>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := Fingerprinter{}
			s := ragel.New("test", strings.NewReader(strings.ToLower(tt.input)), fp)
			got := consumet(t, s, &fp)
			assert.Equal(t, tt.want, got)
		})
	}
}

func BenchmarkFingerprinter1(b *testing.B) {
	input := "[2024-04-06 21:23:32,742] INFO [GroupCoordinator 100]: Preparing to rebalance group metadata.ingest.stats.consumer in state PreparingRebalance with old generation 14 (__consumer_offsets-14) (reason: Adding new member metadata.ingest.stats.consumer-0-e78065b6-0f83-4397-92ae-965997f4b1a2 with group instance id Some(metadata.ingest.stats.consumer-0); client reason: not provided) (kafka.coordinator.group.GroupCoordinator)"
	fingerprinter := &Fingerprinter{}
	s := ragel.New("test", strings.NewReader(strings.ToLower(input)), fingerprinter)
	log.Printf("Running loop for %d times", b.N)
	for i := 0; i < b.N; i++ {
		s.Reset()
		line := consumeb(b, s, fingerprinter)
		xxhash.Sum64String(line)
	}
}

func consumeb(b *testing.B, s *ragel.Scanner, fingerprinter *Fingerprinter) string {
	items := []string{}
	for {
		pos, tok, literal := s.Next()
		switch tok {
		case ragel.EOF:
			return strings.Join(items, " ")
		case ragel.Error:
			b.Fatalf("scan error: %v: %v\n", s.Pos(pos), literal)
		case TokenString:
			items = append(items, literal)
		default:
			items = append(items, "<"+fingerprinter.TokenString(tok)+">")
		}
	}
}
