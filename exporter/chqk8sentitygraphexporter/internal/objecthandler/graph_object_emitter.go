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
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

type GraphObjectEmitter interface {
	Start(ctx context.Context)
	Stop(ctx context.Context)
	Emit(ctx context.Context, object *GraphObject)
}

type graphEmitter struct {
	logger     *zap.Logger
	httpClient *http.Client
	interval   time.Duration
	baseurl    string

	submitChan chan *GraphObject
	doneChan   chan struct{}
	wg         sync.WaitGroup
	objects    []*GraphObject

	decoder runtime.Decoder
}

var _ GraphObjectEmitter = (*graphEmitter)(nil)

type GraphObject struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
}

func NewGraphObjectEmitter(logger *zap.Logger, httpClient *http.Client, interval time.Duration, baseurl string) (GraphObjectEmitter, error) {
	ret := &graphEmitter{
		logger:     logger,
		httpClient: httpClient,
		interval:   interval,
		baseurl:    baseurl,
		doneChan:   make(chan struct{}),
		submitChan: make(chan *GraphObject),
	}

	if err := setupScheme(ret); err != nil {
		return nil, err
	}

	return ret, nil
}

func setupScheme(e *graphEmitter) error {
	scheme := runtime.NewScheme()

	if err := corev1.AddToScheme(scheme); err != nil {
		return err
	}

	if err := appsv1.AddToScheme(scheme); err != nil {
		return err
	}

	if err := batchv1.AddToScheme(scheme); err != nil {
		return err
	}

	if err := networkingv1.AddToScheme(scheme); err != nil {
		return err
	}

	if err := rbacv1.AddToScheme(scheme); err != nil {
		return err
	}

	codecs := serializer.NewCodecFactory(scheme)
	e.decoder = codecs.UniversalDeserializer()

	return nil
}

func (e *graphEmitter) Start(ctx context.Context) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-e.doneChan:
				return
			case object := <-e.submitChan:
				e.objects = append(e.objects, object)
			case <-time.Tick(e.interval):
				err := e.sendObjects(ctx, nil)
				if err != nil {
					e.logger.Error("Failed to send objects", zap.Error(err))
				}
			}
		}
	}()
}

func (e *graphEmitter) Stop(ctx context.Context) {
	close(e.doneChan)
	e.wg.Wait()

	if len(e.objects) > 0 {
		err := e.sendObjects(ctx, nil)
		if err != nil {
			e.logger.Error("Failed to send objects", zap.Error(err))
		}
	}
}

func (e *graphEmitter) Emit(ctx context.Context, object *GraphObject) {
	e.submitChan <- object
}

func (e *graphEmitter) sendObjects(_ context.Context, _ []*GraphObject) error {
	return nil
}
