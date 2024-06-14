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
	"time"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/filereader"
)

type ConfigManager interface {
	RegisterCallback(name string, callback ConfigUpdateCallbackFunc) int
	UnregisterCallback(id int)
	Run()
	Stop()
}

type ConfigUpdateCallbackFunc func(config SamplerConfig)

type ConfigManagerImpl struct {
	done                  chan struct{}
	logger                *zap.Logger
	callbackCounter       int
	callbacks             map[int]ConfigUpdateCallbackFunc
	callbackNames         map[int]string
	registerCallbackChan  chan registerRequest
	unregisterRequestChan chan unregisterRequest
	lasthash              uint64
	freader               filereader.FileReader
	interval              time.Duration
	lastconf              *SamplerConfig
}

var _ ConfigManager = (*ConfigManagerImpl)(nil)

func NewConfigManagerImpl(logger *zap.Logger, CheckInterval time.Duration, fr filereader.FileReader) *ConfigManagerImpl {
	if CheckInterval == 0 {
		CheckInterval = 10 * time.Second
	}
	return &ConfigManagerImpl{
		done:                  make(chan struct{}),
		logger:                logger.Named("sampler_config_manager"),
		callbacks:             map[int]ConfigUpdateCallbackFunc{},
		callbackNames:         map[int]string{},
		registerCallbackChan:  make(chan registerRequest),
		unregisterRequestChan: make(chan unregisterRequest),
		interval:              CheckInterval,
		freader:               fr,
	}
}

type registerRequest struct {
	callback ConfigUpdateCallbackFunc
	name     string
	ret      chan int
}

func (c *ConfigManagerImpl) RegisterCallback(name string, callback ConfigUpdateCallbackFunc) int {
	respchan := make(chan int)
	c.registerCallbackChan <- registerRequest{
		callback: callback,
		name:     name,
		ret:      respchan,
	}
	return <-respchan
}

type unregisterRequest struct {
	id int
}

func (c *ConfigManagerImpl) UnregisterCallback(id int) {
	c.unregisterRequestChan <- unregisterRequest{id: id}
}

func (c *ConfigManagerImpl) Run() {
	first := true
	interval := c.interval
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	c.logger.Info("Starting sampling config manager")
	refreshCount := 0
	for {
		select {
		case <-c.done:
			c.logger.Info("Stopping sampling config manager")
			return
		case <-ticker.C:
			if first {
				first = false
				ticker.Reset(interval)
			}
			c.checkUpdates()
			refreshCount++
			if refreshCount%10 == 0 {
				c.updateCallbacksJustInCase()
			}
		case req := <-c.registerCallbackChan:
			c.register(req)
		case req := <-c.unregisterRequestChan:
			c.unregister(req)
		}
	}
}

func (c *ConfigManagerImpl) Stop() {
	close(c.done)
}

func (c *ConfigManagerImpl) updateCallbacksJustInCase() {
	if c.lastconf == nil {
		return
	}
	for _, callback := range c.callbacks {
		callback(*c.lastconf)
	}
}

func (c *ConfigManagerImpl) register(req registerRequest) {
	c.callbackCounter++
	c.logger.Info("registering callback", zap.Int("id", c.callbackCounter), zap.String("name", req.name))
	c.callbacks[c.callbackCounter] = req.callback
	c.callbackNames[c.callbackCounter] = req.name
	req.ret <- c.callbackCounter
	if c.lastconf != nil {
		c.logger.Info("calling callback", zap.Int("id", c.callbackCounter))
		req.callback(*c.lastconf)
	}
}

func (c *ConfigManagerImpl) unregister(req unregisterRequest) {
	c.logger.Debug("unregistering callback", zap.Int("id", req.id), zap.String("name", c.callbackNames[req.id]))
	delete(c.callbacks, req.id)
	delete(c.callbackNames, req.id)
}

func (c *ConfigManagerImpl) checkUpdates() {
	var conf SamplerConfig

	ll := c.logger.With(zap.String("filename", c.freader.Filename()))

	ll.Debug("Checking for sampler config updates")

	b, err := c.freader.ReadFile(context.Background())
	if err != nil {
		ll.Info("Cannot read sampler config", zap.Error(err))
		return
	}

	newhash := xxhash.Sum64(b)
	if newhash == c.lasthash {
		ll.Debug("No change in sampler config", zap.Uint64("hash", newhash))
		return
	}
	ll.Info("Sampler config updated", zap.Uint64("hash", newhash))

	if err := yaml.Unmarshal(b, &conf); err != nil {
		ll.Error("Error unmarshalling sampler config YAML", zap.Error(err))
		return
	}

	c.lasthash = newhash
	c.lastconf = &conf

	for _, callback := range c.callbacks {
		ll.Info("Calling callback", zap.Int("id", c.callbackCounter))
		callback(conf)
	}
}
