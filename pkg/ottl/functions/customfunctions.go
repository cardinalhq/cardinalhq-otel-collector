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

import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

func factories[K any]() []ottl.Factory[K] {
	return []ottl.Factory[K]{
		NewCidrMatchFactory[K](),
		NewIpLocationFactory[K](),
		NewIsInFactory[K](),
		NewExistsFactory[K](),
		NewDeriveSourceTypeFactory[K](),
		NewLookupFactory[K](),
	}
}

func CustomFunctions[K any]() map[string]ottl.Factory[K] {
	return ottl.CreateFactoryMap(factories[K]()...)
}
