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
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewRPSSampler(t *testing.T) {
	sampler := NewRPSSampler()
	assert.NotNil(t, sampler)
	assert.Equal(t, 30*time.Second, sampler.clearFrequencyDuration)
	assert.Equal(t, 50, sampler.MaxRPS)
	assert.Nil(t, sampler.logger)
}

func TestNewRPSSamplerWithOptions(t *testing.T) {
	clearDuration := 10 * time.Second
	maxRPS := 100
	logger := zap.NewNop()

	sampler := NewRPSSampler(
		WithClearFrequencyDuration(clearDuration),
		WithMaxRPS(maxRPS),
		WithLogger(logger),
	)

	assert.NotNil(t, sampler)
	assert.Equal(t, clearDuration, sampler.clearFrequencyDuration)
	assert.Equal(t, maxRPS, sampler.MaxRPS)
	assert.Equal(t, logger, sampler.logger)
}
