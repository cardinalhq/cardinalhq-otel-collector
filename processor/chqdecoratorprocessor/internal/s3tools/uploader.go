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

package s3tools

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const partSize = 5 * 1024 * 1024

func (u *S3Client) Upload(ctx context.Context, filename string, bucketName string, key string, mimeType string, metadata map[string]string) error {
	uploader := manager.NewUploader(u.S3Client, func(u *manager.Uploader) {
		u.PartSize = partSize
	})
	infile, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	defer infile.Close()
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		Body:        infile,
		ContentType: aws.String(mimeType),
		Metadata:    metadata,
	})
	return err
}

func (u *S3Client) Download(ctx context.Context, bucketName string, key string, dst io.WriterAt) error {
	downloader := manager.NewDownloader(u.S3Client, func(u *manager.Downloader) {
		u.PartSize = partSize
	})
	_, err := downloader.Download(ctx, dst, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	return err
}
