package chqdecoratorprocessor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdate(t *testing.T) {
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
