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

package piiredactionprocessor

import (
	"errors"

	"github.com/cardinalhq/oteltools/pkg/pii"
)

type Config struct {
	Detectors []string `mapstructure:"detectors"`
}

var (
	ErrUnknownDetector = errors.New("unknown detector")
)

func mapDetectorNameToType(name string) (t pii.PIIType, err error) {
	switch name {
	case "email":
		t = pii.PIITypeEmail
	case "ipv4":
		t = pii.PIITypeIPv4
	case "ssn":
		t = pii.PIITypeSSN
	case "phone":
		t = pii.PIITypePhone
	case "ccn":
		t = pii.PIITypeCCN
	default:
		err = ErrUnknownDetector
	}

	return
}

func (c *Config) Validate() error {
	for _, detector := range c.Detectors {
		if _, err := mapDetectorNameToType(detector); err != nil {
			return err
		}
	}

	return nil
}
