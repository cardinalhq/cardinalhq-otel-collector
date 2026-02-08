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

package table

import (
	"github.com/cardinalhq/oteltools/pkg/authenv"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/idgen"
)

type Translator interface {
	LogsFromOtel(ol *plog.Logs, environment authenv.Environment) ([]map[string]any, error)
	MetricsFromOtel(om *pmetric.Metrics, environment authenv.Environment) ([]map[string]any, error)
	TracesFromOtel(ot *ptrace.Traces, environment authenv.Environment) ([]map[string]any, error)
}

type TableTranslator struct {
	idg idgen.IDGenerator
}

var _ Translator = (*TableTranslator)(nil)

func NewTableTranslator() *TableTranslator {
	return &TableTranslator{
		idg: idgen.NewXIDGenerator(),
	}
}
