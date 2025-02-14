// Copyright 2024-2025 CardinalHQ, Inc
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
	"testing"

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/stretchr/testify/assert"
)

func TestMakeFingerprintMapForConfig(t *testing.T) {
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
			result := makeFingerprintMapForConfig(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetTenantUnlocked(t *testing.T) {
	tests := []struct {
		name     string
		cid      string
		setup    func(p *fingerprintProcessor)
		expected *tenantState
	}{
		{
			name: "tenant exists",
			cid:  "tenant1",
			setup: func(p *fingerprintProcessor) {
				p.tenants["tenant1"] = &tenantState{
					mapstore:   NewMapStore(),
					estimators: make(map[uint64]*SlidingEstimatorStat),
				}
			},
			expected: &tenantState{
				mapstore:   NewMapStore(),
				estimators: make(map[uint64]*SlidingEstimatorStat),
			},
		},
		{
			name:  "tenant does not exist",
			cid:   "tenant2",
			setup: func(p *fingerprintProcessor) {},
			expected: &tenantState{
				mapstore:   NewMapStore(),
				estimators: make(map[uint64]*SlidingEstimatorStat),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fingerprintProcessor{
				tenants: make(map[string]*tenantState),
			}
			tt.setup(p)

			result := p.getTenantUnlocked(tt.cid)
			assert.NotNil(t, result)
			assert.NotNil(t, result.mapstore)
			assert.NotNil(t, result.estimators)
		})
	}
}

func TestNormalizeSQL(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{
			input:    `SELECT * FROM users WHERE name = "John Doe" AND age = 30;`,
			expected: `SELECT * FROM users WHERE name = ? AND age = ?;`,
		},
		{
			input:    `INSERT INTO products (id, name, price) VALUES (42, 'Laptop', 999.99);`,
			expected: `INSERT INTO products (id, name, price) VALUES (?, ?, ?);`,
		},
		{
			input:    `UPDATE logs SET message = 'Error occurred at 12:34:56' WHERE id = 1234;`,
			expected: `UPDATE logs SET message = ? WHERE id = ?;`,
		},
		{
			input:    `DELETE FROM orders WHERE order_id = 'ORD-5678' AND customer_id = 789;`,
			expected: `DELETE FROM orders WHERE order_id = ? AND customer_id = ?;`,
		},
		{
			input: `SELECT c.customer_id, c.name, o.order_id, o.total_amount 
					FROM customers c 
					JOIN orders o ON c.customer_id = o.customer_id 
					WHERE c.region = 'US' 
					AND o.order_date >= '2024-01-01' 
					AND o.total_amount > 500;`,
			expected: `SELECT c.customer_id, c.name, o.order_id, o.total_amount 
					FROM customers c 
					JOIN orders o ON c.customer_id = o.customer_id 
					WHERE c.region = ? 
					AND o.order_date >= ? 
					AND o.total_amount > ?;`,
		},
		{
			input: `SELECT p.product_name, p.price, 
					(SELECT COUNT(*) FROM reviews r WHERE r.product_id = p.product_id AND r.rating >= 4) AS positive_reviews 
					FROM products p 
					WHERE p.category = 'Electronics' 
					AND p.stock > 10 
					AND p.price BETWEEN 100 AND 1000;`,
			expected: `SELECT p.product_name, p.price, 
					(SELECT COUNT(*) FROM reviews r WHERE r.product_id = p.product_id AND r.rating >= ?) AS positive_reviews 
					FROM products p 
					WHERE p.category = ? 
					AND p.stock > ? 
					AND p.price BETWEEN ? AND ?;`,
		},
	}

	for _, tc := range testCases {
		normalized := normalizeSQL(tc.input)
		assert.Equal(t, tc.expected, normalized, "Failed to normalize SQL query: %s", tc.input)
	}
}
