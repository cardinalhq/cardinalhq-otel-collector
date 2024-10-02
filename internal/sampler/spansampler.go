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

import (
	"context"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"sync"
)

type SpanSampler interface {
	Sample(span ptrace.Span, fingerprint string, statusCode string, isSlow bool) (droppingRule string)
	UpdateConfig(config *SamplerConfig, vendor string)
}

var _ SpanSampler = (*SpanSamplerImpl)(nil)

type SpanSamplerImpl struct {
	sync.RWMutex
	rules map[string]*spanRule

	logger *zap.Logger
}

func NewSpanSamplerImpl(ctx context.Context, logger *zap.Logger) *SpanSamplerImpl {
	ls := &SpanSamplerImpl{
		logger: logger,
		rules:  map[string]*spanRule{},
	}
	return ls
}

type spanRule struct {
	id       string
	ruleType LogRuleType
	sampler  Sampler
	config   LogSamplingConfigV1
}

func (s SpanSamplerImpl) Sample(span ptrace.Span, fingerprint string, statusCode string, isSlow bool) (droppingRule string) {
	return ""
}

func (s SpanSamplerImpl) UpdateConfig(config *SamplerConfig, vendor string) {

}
