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
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/oschwald/geoip2-golang"
	"github.com/stretchr/testify/assert"
)

// City is the mock implementation of geoip2.Reader's City method
func createMockGeoIP2DB() *geoip2.Reader {
	dbPath := testFile("GeoLite2-City.mmdb")
	db, err := geoip2.Open(dbPath)
	if err != nil {
		println(err.Error())
		panic(err)
	}
	return db
}

func testFile(file string) string {
	return filepath.Join("metadata", file)
}

func Test_IpLocation_ValidIP(t *testing.T) {
	db := createMockGeoIP2DB()

	exprFunc := iplocation[any](db, &ottl.StandardStringGetter[any]{
		Getter: func(context.Context, any) (any, error) {
			return "73.202.180.160", nil
		},
	})

	result, err := exprFunc(context.Background(), nil)
	assert.NoError(t, err)

	expected := map[string]any{
		"city":      "Walnut Creek",
		"country":   "United States",
		"latitude":  37.9164,
		"longitude": -122.0668,
	}

	assert.Equal(t, expected, result)
}
