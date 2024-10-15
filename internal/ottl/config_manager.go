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

package ottl

import (
	"context"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/filereader"
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
		logger:                logger,
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

func (c *ConfigManagerImpl) register(req registerRequest) {
	c.callbackCounter++
	c.logger.Info("registering callback", zap.Int("id", c.callbackCounter), zap.String("name", req.name))
	c.callbacks[c.callbackCounter] = req.callback
	c.callbackNames[c.callbackCounter] = req.name
	req.ret <- c.callbackCounter
	if c.lastconf != nil {
		c.logger.Info("Calling callback (register)", zap.Int("id", c.callbackCounter), zap.String("name", c.callbackNames[c.callbackCounter]))
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

	c.logger.Debug("Checking for sampler config updates")

	b, err := c.freader.ReadFile(context.Background())
	if err != nil {
		c.logger.Info("Cannot read sampler config", zap.Error(err))
		return
	}

	newhash := xxhash.Sum64(b)
	if c.lastconf != nil && c.lastconf.hash == newhash {
		c.logger.Debug("No change in sampler config", zap.Uint64("hash", newhash))
		return
	}
	c.logger.Info("Sampler config updated", zap.Uint64("hash", newhash))
	if err := yaml.Unmarshal(b, &conf); err != nil {
		c.logger.Error("Error unmarshalling sampler config YAML", zap.Error(err))
		return
	}
	conf.hash = newhash
	c.lastconf = &conf

	for callbackId, callback := range c.callbacks {
		c.logger.Info("Calling callback", zap.Int("id", callbackId), zap.String("name", c.callbackNames[callbackId]))
		callback(conf)
	}
}
