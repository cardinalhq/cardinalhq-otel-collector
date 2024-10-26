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

package functions

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/oschwald/geoip2-golang"
)

// IpLocationArguments holds the target IP
type IpLocationArguments[K any] struct {
	Target ottl.StringGetter[K] // Target is the IP address to locate
}

// NewIpLocationFactory Create the factory for the iplocation function
func NewIpLocationFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("IpLocation", &IpLocationArguments[K]{}, func(ctx ottl.FunctionContext, args ottl.Arguments) (ottl.ExprFunc[K], error) {
		ipArgs, ok := args.(*IpLocationArguments[K])
		if !ok {
			return nil, fmt.Errorf("iplocation args must be of type *IpLocationArguments[K]")
		}

		dir, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("error getting working directory: %v", err)
		}
		dbPath := filepath.Join(dir, "functions", "metadata", "GeoLite2-City.mmdb")
		return createIpLocationFunction[K](dbPath, ipArgs.Target)
	})
}

// createIpLocationFunction initializes the GeoIP reader and defines the geolocation logic
func createIpLocationFunction[K any](dbPath string, ipGetter ottl.StringGetter[K]) (ottl.ExprFunc[K], error) {
	db, err := geoip2.Open(dbPath) // Open the GeoLite2 database
	if err != nil {
		return nil, fmt.Errorf("failed to open GeoIP2 database: %v", err)
	}

	return iplocation(db, ipGetter), nil
}

// iplocation function performs the geolocation lookup
func iplocation[K any](db *geoip2.Reader, ipGetter ottl.StringGetter[K]) ottl.ExprFunc[K] {
	return func(ctx context.Context, tCtx K) (any, error) {
		// Get the IP address from the target
		ip, err := ipGetter.Get(ctx, tCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to get IP address: %v", err)
		}

		parsedIP := net.ParseIP(ip)
		if parsedIP == nil {
			return nil, fmt.Errorf("invalid IP address: %s", ip)
		}

		record, err := db.City(parsedIP)
		if err != nil {
			return nil, fmt.Errorf("error looking up IP: %v", err)
		}

		city := record.City.Names["en"]
		country := record.Country.Names["en"]

		if city == "" {
			city = "Unknown"
		}

		if country == "" {
			country = "Unknown"
		}

		location := map[string]any{
			"city":      city,
			"country":   country,
			"latitude":  record.Location.Latitude,
			"longitude": record.Location.Longitude,
		}

		return location, nil
	}
}
