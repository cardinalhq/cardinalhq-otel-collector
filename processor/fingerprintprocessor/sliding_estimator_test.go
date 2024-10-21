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

package fingerprintprocessor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOnlineWindowStat_Update(t *testing.T) {
	episilon := 0.00000000001
	o := NewOnlineWindowStat(5)

	// Set initial state.
	o.Update(1.0)
	assert.Equal(t, 0.0, o.variance())
	o.Update(1.0)
	o.Update(1.0)
	o.Update(1.0)
	o.Update(1.0)

	// Test updating with valid values
	o.Update(1.0)
	assert.Equal(t, 1.0, o.getMean())
	assert.Equal(t, 0.0, o.stdDev())

	o.Update(2.0)
	assert.Equal(t, 1.0, o.getMean())
	assert.InEpsilon(t, 0.4472135954999579, o.stdDev(), episilon)

	o.Update(3.0)
	assert.Equal(t, 2.0, o.getMean())
	assert.InEpsilon(t, 0.8944271909999157, o.stdDev(), episilon)

	o.Update(4.0)
	assert.Equal(t, 2.0, o.getMean())
	assert.InEpsilon(t, 1.3038404810405295, o.stdDev(), episilon)

	o.Update(5.0)
	assert.Equal(t, 3.0, o.getMean())
	assert.InEpsilon(t, 1.5811388300841893, o.stdDev(), episilon)

	// Test updating with NaN value
	o.Update(math.NaN())
	assert.Equal(t, 3.0, o.getMean())
	assert.InEpsilon(t, 1.5811388300841893, o.stdDev(), episilon)

	assert.True(t, o.GreaterThanThreeStdDev(100.0))
	assert.True(t, o.GreaterThanThreeStdDev(3.0+3.0*1.59))
	assert.False(t, o.GreaterThanThreeStdDev(3.0+3.0*1.57))
	assert.False(t, o.GreaterThanThreeStdDev(0.0))
}

func TestSlidingEstimator_Update(t *testing.T) {
	e := NewSlidingEstimator(1_000)

	// Set initial state.
	_, emit := e.Update(1_000, 1.0)
	assert.False(t, emit)

	_, emit = e.Update(1_001, 1.0)
	assert.False(t, emit)

	_, emit = e.Update(1_002, 1.0)
	assert.False(t, emit)

	_, emit = e.Update(1_003, 1.0)
	assert.False(t, emit)

	_, emit = e.Update(1_999, 1.0)
	assert.False(t, emit)

	v, emit := e.Update(2_000, 1.0)
	assert.True(t, emit)
	assert.Equal(t, 1.0, v)
}
