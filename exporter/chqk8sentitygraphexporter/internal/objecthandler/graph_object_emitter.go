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

package objecthandler

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

type GraphObjectEmitter interface {
	Start(ctx context.Context)
	Stop(ctx context.Context)
	Emit(ctx context.Context, object GraphObject)
}

type graphEmitter struct {
	logger         *zap.Logger
	httpClient     *http.Client
	reportInterval time.Duration
	wg             sync.WaitGroup

	donechan chan struct{}
}

var _ GraphObjectEmitter = (*graphEmitter)(nil)

type GraphObject struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
}

func NewGraphObjectEmitter(logger *zap.Logger, httpClient *http.Client, interval time.Duration) GraphObjectEmitter {
	return &graphEmitter{
		logger:         logger,
		httpClient:     httpClient,
		reportInterval: interval,
		donechan:       make(chan struct{}),
	}
}

func (e *graphEmitter) Start(ctx context.Context) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-e.donechan:
				return
			case <-time.Tick(e.reportInterval):
				// do something
			}
		}
	}()
}

func (e *graphEmitter) Stop(ctx context.Context) {
	close(e.donechan)
	e.wg.Wait()
}

func (e *graphEmitter) Emit(ctx context.Context, object GraphObject) {
}
