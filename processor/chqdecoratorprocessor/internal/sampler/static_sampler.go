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
	"github.com/honeycombio/dynsampler-go"
)

type StaticSampler struct {
	fixedRate int
}

var _ dynsampler.Sampler = (*StaticSampler)(nil)

func NewStaticSampler(sampleRate float64) *StaticSampler {
	rate := 0
	if sampleRate > 0 {
		rate = int(1 / sampleRate)
	}
	return &StaticSampler{
		fixedRate: rate,
	}
}

func (s *StaticSampler) Start() error {
	return nil
}

func (s *StaticSampler) Stop() error {
	return nil
}

func (s *StaticSampler) GetSampleRate(_ string) int {
	return s.fixedRate
}

func (s *StaticSampler) GetSampleRateMulti(_ string, _ int) int {
	return s.fixedRate
}

func (s *StaticSampler) SaveState() ([]byte, error) {
	return nil, nil
}

func (s *StaticSampler) LoadState(state []byte) error {
	return nil
}

func (s *StaticSampler) GetMetrics(prefix string) map[string]int64 {
	return map[string]int64{
		prefix + ".fixed_rate": int64(s.fixedRate),
	}
}
