package fingerprintprocessor

import (
	"testing"

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/stretchr/testify/assert"
)

func TestMakeFingerprintMap(t *testing.T) {
	tests := []struct {
		name     string
		input    []ottl.FingerprintMapping
		expected map[int64]int64
	}{
		{
			name: "single mapping",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{2, 3},
				},
			},
			expected: map[int64]int64{
				2: 1,
				3: 1,
			},
		},
		{
			name: "multiple mappings",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{2, 3},
				},
				{
					Primary: 4,
					Aliases: []int64{5, 6},
				},
			},
			expected: map[int64]int64{
				2: 1,
				3: 1,
				5: 4,
				6: 4,
			},
		},
		{
			name:     "empty input",
			input:    []ottl.FingerprintMapping{},
			expected: map[int64]int64{},
		},
		{
			name: "no aliases",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{},
				},
			},
			expected: map[int64]int64{},
		},
		{
			name:     "nil input",
			input:    nil,
			expected: map[int64]int64{},
		},
		{
			name: "nil aliases",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: nil,
				},
			},
			expected: map[int64]int64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := makeFingerprintMap(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateMapHash(t *testing.T) {
	tests := []struct {
		name     string
		input    []ottl.FingerprintMapping
		expected int64
	}{
		{
			name: "single mapping",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{2, 3},
				},
			},
			expected: -2725809024417325307,
		},
		{
			name: "multiple mappings",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{2, 3},
				},
				{
					Primary: 4,
					Aliases: []int64{5, 6},
				},
			},
			expected: -6635677175182172702,
		},
		{
			name: "multiple mappings with primary keys out of order, and aliases out of order",
			input: []ottl.FingerprintMapping{
				{
					Primary: 4,
					Aliases: []int64{5, 6},
				},
				{
					Primary: 1,
					Aliases: []int64{3, 2},
				},
			},
			expected: -6635677175182172702,
		},
		{
			name:     "empty input",
			input:    []ottl.FingerprintMapping{},
			expected: -3750763034362895579,
		},
		{
			name: "no aliases",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{},
				},
			},
			expected: -8517097267634966620,
		},
		{
			name:     "nil input",
			input:    nil,
			expected: -3750763034362895579,
		},
		{
			name: "nil aliases",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: nil,
				},
			},
			expected: -8517097267634966620,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateMapHash(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func BenchmarkCalculateMapHash(b *testing.B) {
	m := []ottl.FingerprintMapping{
		{
			Primary: 1,
			Aliases: []int64{2, 3},
		},
		{
			Primary: 4,
			Aliases: []int64{5, 6},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		calculateMapHash(m)
	}
}

func BenchmarkMakeFingerprintMap(b *testing.B) {
	m := []ottl.FingerprintMapping{
		{
			Primary: 1,
			Aliases: []int64{2, 3},
		},
		{
			Primary: 4,
			Aliases: []int64{5, 6},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		makeFingerprintMap(m)
	}
}
