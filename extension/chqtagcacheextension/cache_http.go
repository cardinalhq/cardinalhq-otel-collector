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

package chqtagcacheextension

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
)

type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type TagsMessage struct {
	Tags []Tag `json:"tags"`
}

func (chq *CHQTagcacheExtension) FetchTags(key string) (any, error) {
	resp, err := chq.httpClient.Get(chq.config.Endpoint + "/api/v1/tags?hostname=" + key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("Failed to fetch tags, error code: " + resp.Status)
	}

	var tags TagsMessage
	err = json.NewDecoder(resp.Body).Decode(&tags)
	if err != nil {
		return nil, err
	}
	return tags.Tags, nil
}

func (chq *CHQTagcacheExtension) PutTags(key string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}

	resp, err := chq.httpClient.Post(chq.config.Endpoint+"/api/v1/tags?hostname="+key, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return errors.New("Failed to put tags, error code: " + resp.Status)
	}

	return nil
}
