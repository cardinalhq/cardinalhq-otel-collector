package functions

import (
	"context"
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type LookupArguments[K any] struct {
	LookupKey ottl.StringGetter[K]
	Lookup    []string // Array in [key1, value1, key2, value2, ...] format
	DefaultTo string
}

func NewLookupFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("Lookup", &LookupArguments[K]{}, createLookupFunction[K])
}

func createLookupFunction[K any](_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*LookupArguments[K])
	if !ok {
		return nil, fmt.Errorf("LookupFactory args must be of type *LookupArguments[K]")
	}

	// Convert the slice to a map for fast lookup
	if len(args.Lookup)%2 != 0 {
		return nil, fmt.Errorf("lookup slice must contain an even number of elements")
	}

	lookupMap := make(map[string]any, len(args.Lookup)/2)
	for i := 0; i < len(args.Lookup); i += 2 {
		key := args.Lookup[i]
		value := args.Lookup[i+1]
		lookupMap[key] = value
	}

	return lookup(args.LookupKey, lookupMap, args.DefaultTo), nil
}

func lookup[K any](lookupKey ottl.StringGetter[K], lookupMap map[string]any, defaultTo string) ottl.ExprFunc[K] {
	return func(ctx context.Context, input K) (any, error) {
		// Retrieve the key from the LookupKey getter
		key, err := lookupKey.Get(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("error retrieving lookup key: %w", err)
		}

		// Check if the key exists in the map
		if value, exists := lookupMap[key]; exists {
			return value, nil
		}

		// Return DefaultTo if the key is not found
		return defaultTo, nil
	}
}
