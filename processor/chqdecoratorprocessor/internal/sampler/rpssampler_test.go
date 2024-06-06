package sampler

import (
	"log"
	"slices"
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

func xTestRPSSampler_Rate(t *testing.T) {
	sampler := NewRPSSampler(WithMaxRPS(50), WithClearFrequencyDuration(30*time.Second))
	err := sampler.Start()
	assert.NoError(t, err)

	defer func() {
		err := sampler.Stop()
		assert.NoError(t, err)
	}()

	values := map[int]int64{}
	for iterations := 0; iterations < 100; iterations++ {
		for i := 0; i < 500000; i++ {
			rps := sampler.GetSampleRate("key")
			values[rps]++
		}
		showValues(iterations, values)
		values = map[int]int64{}
		sampler.updateMaps()
	}

	assert.Fail(t, "forced")
}

func showValues(iteration int, values map[int]int64) {
	keys := []int{}
	for k := range values {
		keys = append(keys, k)
	}

	slices.Sort(keys)
	for _, k := range keys {
		log.Printf("%4d: %6d: %d", iteration, k, values[k])
	}
}
