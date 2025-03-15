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
)

type HandlerFunc func(rlattr pcommon.Map, lattr pcommon.Map, body pcommon.Map) error

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
	h.handlers[objectSelector{"v1", "pod"}] = h.handleV1Pod
}

func (h *handlerImpl) Feed(rlattr pcommon.Map, lattr pcommon.Map, bodyValue pcommon.Value) {
	if bodyValue.Type() != pcommon.ValueTypeMap {
		return
	}
	body := bodyValue.Map()
	APIVersion, ok := body.Get("apiVersion")
	if !ok {
		return
	}
	Kind, ok := body.Get("kind")
	if !ok {
		return
	}

	selector := objectSelector{APIVersion: APIVersion.AsString(), Kind: Kind.AsString()}
	if handler, ok := h.handlers[selector]; ok {
		if err := handler(rlattr, lattr, body); err != nil {
			return
		}
	}
}

func (h *handlerImpl) handleV1Pod(rlattr pcommon.Map, lattr pcommon.Map, body pcommon.Map) error {
	// TODO: Implement this
	return nil
}
