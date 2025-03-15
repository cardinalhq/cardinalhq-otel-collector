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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	convertv1 "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/v1"
)

type HandlerFunc func(rlattr pcommon.Map, lattr pcommon.Map, us unstructured.Unstructured) error

type objectSelector struct {
	APIVersion string
	Kind       string
}

type Handlers map[objectSelector]HandlerFunc

type ObjectHandler interface {
	Feed(rlattr pcommon.Map, lattr pcommon.Map, bodyValue pcommon.Value)
}

type handlerImpl struct {
	handlers      Handlers
	logger        *zap.Logger
	objectEmitter GraphObjectEmitter
	eventEmitter  GraphEventEmitter
}

var _ ObjectHandler = (*handlerImpl)(nil)

func NewObjectHandler(logger *zap.Logger, objectEmitter GraphObjectEmitter, eventEmitter GraphEventEmitter) ObjectHandler {
	ret := &handlerImpl{
		handlers:      Handlers{},
		logger:        logger,
		objectEmitter: objectEmitter,
		eventEmitter:  eventEmitter,
	}
	ret.installHandlers()
	return ret
}

func (h *handlerImpl) installHandlers() {
	h.handlers[objectSelector{"v1", "Pod"}] = h.handleV1Pod
}

func (h *handlerImpl) Feed(rlattr pcommon.Map, lattr pcommon.Map, bodyValue pcommon.Value) {
	if bodyValue.Type() != pcommon.ValueTypeMap {
		return
	}
	body := bodyValue.Map()

	us := unstructured.Unstructured{
		Object: body.AsRaw(),
	}
	APIVersion := us.GetAPIVersion()
	Kind := us.GetKind()

	selector := objectSelector{APIVersion: APIVersion, Kind: Kind}
	handler, ok := h.handlers[selector]
	if !ok {
		h.logger.Info("No handler found for object", zap.String("APIVersion", APIVersion), zap.String("Kind", Kind))
		return
	}
	if err := handler(rlattr, lattr, us); err != nil {
		h.logger.Error("Error handling object", zap.Error(err))
		return
	}
}

func (h *handlerImpl) handleV1Pod(rlattr pcommon.Map, lattr pcommon.Map, us unstructured.Unstructured) error {
	result, err := convertv1.ConvertPod(us)
	if err != nil {
		return err
	}

	h.logger.Info("Pod", zap.Any("result", result))

	return nil
}
