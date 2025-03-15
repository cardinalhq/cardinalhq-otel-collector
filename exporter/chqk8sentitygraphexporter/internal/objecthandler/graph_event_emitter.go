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

type GraphEventEmitter interface {
	Start(ctx context.Context)
	Stop(ctx context.Context)
	Emit(ctx context.Context, event *GraphEvent)
}

type graphEventEmitter struct {
	logger     *zap.Logger
	httpClient *http.Client
	interval   time.Duration
	baseurl    string

	submitChan chan *GraphEvent
	doneChan   chan struct{}
	wg         sync.WaitGroup
	events     []*GraphEvent
}

var _ GraphEventEmitter = (*graphEventEmitter)(nil)

type GraphEvent struct {
	EventType string `json:"event_type"`
	Source    string `json:"source"`
}

func NewGraphEventEmitter(logger *zap.Logger, httpClient *http.Client, interval time.Duration, baseurl string) GraphEventEmitter {
	return &graphEventEmitter{
		logger:     logger,
		httpClient: httpClient,
		interval:   interval,
		baseurl:    baseurl,
		doneChan:   make(chan struct{}),
		submitChan: make(chan *GraphEvent),
	}
}

func (e *graphEventEmitter) Start(ctx context.Context) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-e.doneChan:
				return
			case event := <-e.submitChan:
				e.events = append(e.events, event)
			case <-time.Tick(e.interval):
				err := e.sendEvents(ctx, nil)
				if err != nil {
					e.logger.Error("failed to send events", zap.Error(err))
				}
			}
		}
	}()
}

func (e *graphEventEmitter) Stop(ctx context.Context) {
	close(e.doneChan)
	e.wg.Wait()

	if len(e.events) > 0 {
		err := e.sendEvents(ctx, e.events)
		if err != nil {
			e.logger.Error("failed to send events", zap.Error(err))
		}
	}
}

func (e *graphEventEmitter) Emit(ctx context.Context, event *GraphEvent) {
	e.submitChan <- event
}

func (e *graphEventEmitter) sendEvents(_ context.Context, _ []*GraphEvent) error {
	return nil
}
