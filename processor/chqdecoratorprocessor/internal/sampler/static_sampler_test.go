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

package sampler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStaticSampler(t *testing.T) {
	s := NewStaticSampler(0.5)
	assert.Equal(t, 2, s.fixedRate)
}

func TestStaticSampler_GetSampleRate(t *testing.T) {
	s := NewStaticSampler(0.5)
	assert.Equal(t, 2, s.GetSampleRate(""))
}

func TestStaticSampler_GetSampleRateMulti(t *testing.T) {
	s := NewStaticSampler(0.5)
	assert.Equal(t, 2, s.GetSampleRateMulti("", 1))
}

func TestStaticSampler_SaveState(t *testing.T) {
	s := NewStaticSampler(0.5)
	state, err := s.SaveState()
	assert.NoError(t, err)
	assert.Nil(t, state)
}

func TestStaticSampler_LoadState(t *testing.T) {
	s := NewStaticSampler(0.5)
	err := s.LoadState(nil)
	assert.NoError(t, err)
}

func TestStaticSampler_GetMetrics(t *testing.T) {
	s := NewStaticSampler(0.5)
	metrics := s.GetMetrics("test")
	assert.Equal(t, map[string]int64{
		"test.fixed_rate": 2,
	}, metrics)
}

func TestStaticSampler_Start(t *testing.T) {
	s := NewStaticSampler(0.5)
	err := s.Start()
	assert.NoError(t, err)
}

func TestStaticSampler_Stop(t *testing.T) {
	s := NewStaticSampler(0.5)
	err := s.Stop()
	assert.NoError(t, err)
}

func TestStaticSamplerEdgeCases(t *testing.T) {
	s := NewStaticSampler(0.0)
	assert.Equal(t, 0, s.GetSampleRate(""))
	assert.Equal(t, 0, s.GetSampleRateMulti("", 10))

	s = NewStaticSampler(1.0)
	assert.Equal(t, 1, s.GetSampleRate(""))
	assert.Equal(t, 1, s.GetSampleRateMulti("", 10))
}
