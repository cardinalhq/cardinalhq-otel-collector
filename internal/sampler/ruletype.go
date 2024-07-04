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

package sampler

type LogRuleType int

const (
	LogRuleTypeUnknown LogRuleType = iota
	LogRuleTypeRandom
	LogRuleTypeRPS
)

func logRuletypeToInt(ruleType string) LogRuleType {
	switch ruleType {
	case "random":
		return LogRuleTypeRandom
	case "rps":
		return LogRuleTypeRPS
	default:
		return LogRuleTypeUnknown
	}
}
