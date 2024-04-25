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
	"os"
)

type LocalFileReader struct {
	path string
}

var _ FileReader = (*LocalFileReader)(nil)

func NewLocalFileReader(path string) *LocalFileReader {
	return &LocalFileReader{
		path: path,
	}
}

func (l *LocalFileReader) ReadFile(_ context.Context) ([]byte, error) {
	return os.ReadFile(l.path)
}

func (l *LocalFileReader) Filename() string {
	return "file://" + l.path
}
