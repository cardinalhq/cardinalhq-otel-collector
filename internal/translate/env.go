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

package translate

import (
	"os"
	"strings"
	"sync"
)

const (
	CardinalEnvCustomerID  = "CARDINALHQ_CUSTOMER_ID"
	CardinalEnvCollectorID = "CARDINALHQ_COLLECTOR_ID"
)

type Environment interface {
	CustomerID() string
	CollectorID() string
	Tags() map[string]string
}

type EnvironmentImpl struct {
	tags map[string]string
}

var (
	environmentSetupOnce sync.Once
	environment          *EnvironmentImpl

	_ Environment = (*EnvironmentImpl)(nil)
)

func EnvironmentFromEnv() *EnvironmentImpl {
	environmentSetupOnce.Do(func() {
		environment = environmentFromEnv()
	})
	return environment
}

func environmentFromEnv() *EnvironmentImpl {
	customerid := os.Getenv(CardinalEnvCustomerID)
	collectorid := os.Getenv(CardinalEnvCollectorID)

	tags := make(map[string]string)
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "CARDINALHQ_ENV_") {
			parts := strings.SplitN(v, "=", 2)
			if len(parts) == 2 {
				tagname := strings.TrimPrefix(parts[0], "CARDINALHQ_ENV_")
				tagname = strings.ToLower(tagname)
				tags[tagname] = parts[1]
			}
		}
	}

	tags["customer_id"] = customerid
	tags["collector_id"] = collectorid

	return &EnvironmentImpl{
		tags: tags,
	}
}

func (e *EnvironmentImpl) CustomerID() string {
	return e.tags["customer_id"]
}

func (e *EnvironmentImpl) CollectorID() string {
	return e.tags["collector_id"]
}

func (e *EnvironmentImpl) Tags() map[string]string {
	return e.tags
}
