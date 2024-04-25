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

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/s3tools"
)

type S3FileReader struct {
	s3client   *s3tools.S3Client
	bucketname string
	key        string
}

var _ FileReader = (*S3FileReader)(nil)

func NewS3FileReader(s3client *s3tools.S3Client, bucketname string, key string) *S3FileReader {
	return &S3FileReader{
		s3client:   s3client,
		bucketname: bucketname,
		key:        key,
	}
}

func (s *S3FileReader) ReadFile(ctx context.Context) ([]byte, error) {
	out := manager.NewWriteAtBuffer([]byte{})
	err := s.s3client.Download(ctx, s.bucketname, s.key, out)
	if err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func (s *S3FileReader) Filename() string {
	return "s3://" + s.bucketname + "/" + s.key
}
