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

package datadogreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestPopulateDatapoint(t *testing.T) {
	dp := pmetric.NewNumberDataPoint()
	ts := int64(1629878400) // Example timestamp
	value := 123.45         // Example value

	populateDatapoint(&dp, ts, value)

	assert.Equal(t, pcommon.Timestamp(1629878400*time.Millisecond), dp.Timestamp())
	assert.Equal(t, value, dp.DoubleValue())
	assert.Equal(t, pcommon.Timestamp(1629878340*time.Millisecond), dp.StartTimestamp())
}
