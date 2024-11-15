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
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTPFileReader_ReadFile_WithAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/file.txt", r.URL.Path)

		_, err := w.Write([]byte("test data"))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}))

	defer server.Close()

	reader := NewHTTPFileReader(server.URL+"/file.txt", server.Client())

	data, err := reader.ReadFile(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expectedData := []byte("test data")
	if !bytes.Equal(data, expectedData) {
		t.Errorf("unexpected data: got %s, want %s", data, expectedData)
	}
}

func TestHTTPFileReader_ReadFile_WithoutAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/file.txt", r.URL.Path)

		authHeader := r.Header.Get("x-cardinalhq-api-key")
		if authHeader != "" {
			t.Errorf("unexpected Authorization header: got %s, want empty", authHeader)
		}

		_, err := w.Write([]byte("test data"))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}))

	defer server.Close()

	reader := NewHTTPFileReader(server.URL+"/file.txt", server.Client())

	data, err := reader.ReadFile(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expectedData := []byte("test data")
	if !bytes.Equal(data, expectedData) {
		t.Errorf("unexpected data: got %s, want %s", data, expectedData)
	}
}

func TestHTTPFileReader_Readfile_BadURL(t *testing.T) {
	reader := NewHTTPFileReader("xxx://example.com/file.txt", nil)

	_, err := reader.ReadFile(context.Background())
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestHTTPFileReader_Filename(t *testing.T) {
	reader := &HTTPFileReader{
		URL: "http://example.com/file.txt",
	}

	expectedFilename := "http://example.com/file.txt"
	actualFilename := reader.Filename()

	assert.Equal(t, expectedFilename, actualFilename)
}

func TestNewHTTPFileReader_DefaultClient(t *testing.T) {
	url := "http://example.com/file.txt"

	reader := NewHTTPFileReader(url, nil)

	assert.Equal(t, url, reader.URL)
	assert.Equal(t, http.DefaultClient, reader.client)
}

func TestNewHTTPFileReader_CustomClient(t *testing.T) {
	url := "http://example.com/file.txt"
	customClient := &http.Client{}

	reader := NewHTTPFileReader(url, customClient)

	assert.Equal(t, url, reader.URL)
	assert.Equal(t, customClient, reader.client)
}
