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

package datadogreceiver

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type DDErrorWrapper struct {
	Errors []DDError `json:"errors"`
}

type DDError struct {
	Detail string `json:"detail"`
	Status string `json:"status"`
	Title  string `json:"title"`
}

func writeError(w http.ResponseWriter, code int, err error) {
	e := DDErrorWrapper{
		Errors: []DDError{
			{
				Detail: err.Error(),
				Status: fmt.Sprintf("%d", code),
				Title:  http.StatusText(code),
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(e)
	_, _ = w.Write(b)
}