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

package chqmissingdataconnector

import (
	"fmt"
	"time"
)

var (
	defaultMaximumAge      = 24 * time.Hour
	defaultInterval        = 1 * time.Minute
	defaultNamePrefix      = "missingdata"
	defaultResourcesToCopy = []string{"service.name"}
)

type Config struct {
	MaximumAge                time.Duration `mapstructure:"maximum_age"`
	Interval                  time.Duration `mapstructure:"interval"`
	NamePrefix                string        `mapstructure:"name_prefix"`
	ResourceAtttributesToCopy []string      `mapstructure:"resource_attributes_to_copy"`
}

func (c *Config) Validate() error {
	if c.MaximumAge <= 1*time.Minute {
		return fmt.Errorf("maximum_age must be greater than 1 minute")
	}
	if c.Interval <= 1*time.Second {
		return fmt.Errorf("interval must be greater than 1 second")
	}
	return nil
}
