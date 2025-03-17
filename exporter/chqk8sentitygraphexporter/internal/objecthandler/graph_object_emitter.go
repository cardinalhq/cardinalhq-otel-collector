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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type GraphObjectEmitter interface {
	Start(ctx context.Context)
	Stop(ctx context.Context)
	Upsert(ctx context.Context, object *graphpb.PackagedObject) error
}

type graphEmitter struct {
	logger     *zap.Logger
	httpClient *http.Client
	interval   time.Duration
	baseurl    string

	submitChan chan *graphpb.PackagedObject
	doneChan   chan struct{}
	wg         sync.WaitGroup
	objects    map[string]*WrappedObject
	expiry     time.Duration
}

var _ GraphObjectEmitter = (*graphEmitter)(nil)

type WrappedObject struct {
	obj      *graphpb.PackagedObject
	lastSent time.Time
	lastSeen time.Time
}

func (w *WrappedObject) needsSend(now time.Time, maxage time.Duration) bool {
	return now.Sub(w.lastSent) > maxage
}

func (w *WrappedObject) markSent(now time.Time) {
	w.lastSent = now
}

func (w *WrappedObject) markSeen(now time.Time) {
	w.lastSeen = now
}

func (w *WrappedObject) expired(now time.Time, maxage time.Duration) bool {
	return now.Sub(w.lastSeen) > maxage
}

func NewGraphObjectEmitter(logger *zap.Logger, httpClient *http.Client, interval time.Duration, baseurl string) (GraphObjectEmitter, error) {
	ret := &graphEmitter{
		logger:     logger,
		httpClient: httpClient,
		interval:   interval,
		baseurl:    baseurl + "/api/v1/servicegraph/objects",
		doneChan:   make(chan struct{}),
		submitChan: make(chan *graphpb.PackagedObject, 1000),
		objects:    make(map[string]*WrappedObject),
		expiry:     3 * interval,
	}

	logger.Debug("Created new graph object emitter",
		zap.String("baseurl", ret.baseurl),
		zap.Duration("interval", ret.interval),
		zap.Duration("expiry", ret.expiry))

	return ret, nil
}

func (e *graphEmitter) Start(ctx context.Context) {
	e.logger.Debug("Starting graph object emitter", zap.String("baseurl", e.baseurl), zap.Duration("interval", e.interval))
	e.wg.Add(1)
	ctx = context.WithoutCancel(ctx)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(e.interval)
		defer ticker.Stop()

		for {
			select {
			case <-e.doneChan:
				return
			case object := <-e.submitChan:
				if object == nil || object.Object == nil {
					continue
				}
				bo := object.GetBaseObject()
				if bo == nil {
					continue
				}
				id := bo.GetId()
				if old, found := e.objects[id]; found &&
					old.obj.GetBaseObject().GetResourceVersion() == bo.GetResourceVersion() &&
					old.obj.GetBaseObject().GetUid() == bo.GetUid() {
					old.markSeen(time.Now())
					continue
				}
				e.objects[id] = &WrappedObject{
					obj:      object,
					lastSeen: time.Now(),
				}
			case <-ticker.C:
				err := e.sendObjects(ctx)
				if err != nil {
					e.logger.Info("Failed to send objects", zap.Any("error", err))
				}
			}
		}
	}()
}

func (e *graphEmitter) Stop(ctx context.Context) {
	close(e.doneChan)
	e.wg.Wait()

	err := e.sendObjects(ctx)
	if err != nil {
		e.logger.Error("Failed to send objects", zap.Error(err))
	}
}

// Upsert adds or updates an object in the emitter queue.
// If the context is cancelled, the function returns immediately, and the
// object is not added to the queue.
func (e *graphEmitter) Upsert(ctx context.Context, object *graphpb.PackagedObject) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.submitChan <- object:
		return nil
	}
}

func (e *graphEmitter) selectToSend() []*graphpb.PackagedObject {
	toSend := make([]*graphpb.PackagedObject, 0, len(e.objects))
	now := time.Now()
	for k, obj := range e.objects {
		if obj.expired(now, e.expiry) {
			delete(e.objects, k)
			continue
		}
		if obj.needsSend(now, e.expiry) {
			toSend = append(toSend, obj.obj)
			obj.markSent(now) // mark as sent even if we fail to send
		}
	}
	return toSend
}

func (e *graphEmitter) sendObjects(ctx context.Context) error {
	if len(e.objects) == 0 {
		return nil
	}

	toSend := e.selectToSend()
	if len(toSend) == 0 {
		return nil
	}
	list := &graphpb.PackagedObjectList{
		Items: toSend,
	}

	b, err := proto.Marshal(list)
	if err != nil {
		return err
	}

	e.logger.Debug("Sending objects", zap.Int("count", len(toSend)), zap.Int("size", len(b)))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseurl, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	return nil
}
