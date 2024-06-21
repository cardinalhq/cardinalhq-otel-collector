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

package filereader

import (
	"context"
	"io"
	"net/http"
)

type HTTPFileReader struct {
	// URL is the URL of the file to read
	URL string

	client *http.Client
}

var _ FileReader = (*HTTPFileReader)(nil)

func NewHTTPFileReader(url string, client *http.Client) *HTTPFileReader {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPFileReader{
		URL:    url,
		client: client,
	}
}

func (r *HTTPFileReader) ReadFile(ctx context.Context) (data []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (r *HTTPFileReader) Filename() string {
	return r.URL
}
