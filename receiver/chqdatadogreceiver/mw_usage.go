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

package datadogreceiver

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type MiddlewareUsage struct {
	counter metric.Int64Counter
	next    http.Handler
}

func NewMiddlewareUsage(counter metric.Int64Counter, next http.Handler) *MiddlewareUsage {
	return &MiddlewareUsage{
		counter: counter,
	}
}

func (mu *MiddlewareUsage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	w = newResponseWriter(w)
	mu.next.ServeHTTP(w, r)

	mu.counter.Add(r.Context(), 1, metric.WithAttributes(
		attribute.String("path", path),
		attribute.Int("status_code", w.(*responseWriter).status),
	))
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	return rw.ResponseWriter.Write(b)
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w}
}
