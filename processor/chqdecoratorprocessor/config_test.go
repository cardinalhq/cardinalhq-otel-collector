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

package chqdecoratorprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTraceConfig_Validate(t *testing.T) {
	//one := int(1)
	//zero := int(0)

	tests := []struct {
		name      string
		config    *TraceConfig
		wantError bool
	}{
		{
			"valid",
			&TraceConfig{
				GraphURL: "http://example.com",
			},
			false,
		},
		{
			"invalid URL",
			&TraceConfig{
				GraphURL: "://example.com",
			},
			true,
		},
		{
			"unsupported scheme",
			&TraceConfig{
				GraphURL: "ftp://example.com",
			},
			true,
		},
		{
			"no graphurl",
			&TraceConfig{},
			false,
		},
		{
			"invalid uninteresting rate",
			&TraceConfig{
				UninterestingRate: &[]int{-1}[0],
			},
			true,
		},
		{
			"invalid slow rate",
			&TraceConfig{
				SlowRate: &[]int{-1}[0],
			},
			true,
		},
		{
			"invalid has error rate",
			&TraceConfig{
				HasErrorRate: &[]int{-1}[0],
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.wantError, err != nil)
		})
	}
}

func TestTraceConfig_ValidateAppliesDefaults(t *testing.T) {
	cfg := &TraceConfig{}
	err := cfg.Validate()
	require.NoError(t, err)
	require.NotNil(t, cfg.UninterestingRate)
	require.NotNil(t, cfg.SlowRate)
	require.NotNil(t, cfg.HasErrorRate)
}

func TestCheckSamplerConfigFile(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		wantErr bool
	}{
		{
			"valid file scheme",
			"file:///path/to/config.yaml",
			false,
		},
		{
			"valid http scheme",
			"http://example.com/config.yaml",
			false,
		},
		{
			"valid https scheme",
			"https://example.com/config.yaml",
			false,
		},
		{
			"invalid scheme",
			"ftp://example.com/config.yaml",
			true,
		},
		{
			"missing scheme",
			"/path/to/config.yaml",
			true,
		},
		{
			"invalid URL",
			"://example.com/config.yaml",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSamplerConfigFile(tt.file)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkSamplerConfigFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
