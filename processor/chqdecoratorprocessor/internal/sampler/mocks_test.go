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

import "github.com/honeycombio/dynsampler-go"

type MockSampler struct {
	nextret int
	values  []int
}

var _ dynsampler.Sampler = (*MockSampler)(nil)

func (m *MockSampler) Start() error {
	return nil
}

func (m *MockSampler) Stop() error {
	return nil
}

func (m *MockSampler) GetSampleRate(key string) int {
	ret := m.values[m.nextret]
	m.nextret++
	if m.nextret >= len(m.values) {
		m.nextret = len(m.values) - 1
	}
	return ret
}

func (m *MockSampler) GetSampleRateMulti(key string, n int) int {
	return m.GetSampleRate(key)
}

func (m *MockSampler) SaveState() ([]byte, error) {
	return nil, nil
}

func (m *MockSampler) LoadState(state []byte) error {
	return nil
}

func (m *MockSampler) GetMetrics(prefix string) map[string]int64 {
	return nil
}

//
// MockRateSampler is a mock implementation of the dynsampler.Sampler interface
// that returns a fixed rate for all keys.
//

type MockRateSampler struct {
	rate int
}

var _ dynsampler.Sampler = (*MockRateSampler)(nil)

func (m *MockRateSampler) Start() error {
	return nil
}

func (m *MockRateSampler) Stop() error {
	return nil
}

func (m *MockRateSampler) GetSampleRate(key string) int {
	return m.rate
}

func (m *MockRateSampler) GetSampleRateMulti(key string, n int) int {
	return m.rate
}

func (m *MockRateSampler) SaveState() ([]byte, error) {
	return nil, nil
}

func (m *MockRateSampler) LoadState(state []byte) error {
	return nil
}

func (m *MockRateSampler) GetMetrics(prefix string) map[string]int64 {
	return nil
}