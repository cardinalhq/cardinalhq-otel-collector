// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type s3Writer struct{}

type filewriter interface {
	writeBuffer(ctx context.Context, now time.Time, buf io.Reader, config *Config, metadata string, format string, kv map[string]string, customerID string) error
}

var _ filewriter = (*s3Writer)(nil)

// generate the s3 time key based on partition configuration
func getTimeKey(time time.Time, partition string, customerID string) string {
	var timeKey string
	year, month, day := time.Date()
	hour, minute, _ := time.Clock()

	if partition == "hour" {
		timeKey = fmt.Sprintf("year=%d/month=%02d/day=%02d/hour=%02d", year, month, day, hour)
	} else {
		timeKey = fmt.Sprintf("year=%d/month=%02d/day=%02d/hour=%02d/minute=%02d", year, month, day, hour, minute)
	}
	if customerID != "" {
		timeKey = customerID + "/" + timeKey
	}

	return timeKey
}

func randomInRange(low, hi int) int {
	return low + rand.Intn(hi-low)
}

func getS3Key(time time.Time, keyPrefix string, partition string, filePrefix string, metadata string, fileFormat string, customerID string) string {
	timeKey := getTimeKey(time, partition, customerID)
	randomID := randomInRange(100000000, 999999999)
	suffix := ""
	if fileFormat != "" {
		suffix = "." + fileFormat
	}

	s3Key := keyPrefix + "/" + timeKey + "/" + filePrefix + metadata + "_" + strconv.Itoa(randomID) + suffix

	return s3Key
}

func getS3Client(ctx context.Context, cfg *Config, customerID string) (*s3.Client, error) {
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(cfg.S3Uploader.Region))

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	var s3Opts []func(*s3.Options)

	if cfg.S3Uploader.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.S3Uploader.Endpoint)
		})
	}

	if cfg.S3Uploader.S3ForcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	if cfg.S3Uploader.DisableSSL {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.EndpointOptions.DisableHTTPS = true
		})
	}

	if cfg.S3Uploader.RoleArn != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		creds := stscreds.NewAssumeRoleProvider(stsClient, cfg.S3Uploader.RoleArn, func(o *stscreds.AssumeRoleOptions) {
			o.ExternalID = aws.String(customerID)
		})
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.Credentials = aws.NewCredentialsCache(creds)
		})
	}

	return s3.NewFromConfig(awsCfg, s3Opts...), nil
}

func (s3writer *s3Writer) writeBuffer(ctx context.Context, now time.Time, buf io.Reader, cfg *Config, metadata string, format string, kv map[string]string, customerID string) error {
	key := getS3Key(now,
		cfg.S3Uploader.S3Prefix, cfg.S3Uploader.S3Partition,
		cfg.S3Uploader.FilePrefix, metadata, format, customerID)

	contentType := ""
	if format == "parquet" {
		contentType = "application/vnd.apache.parquet"
	}

	client, err := getS3Client(ctx, cfg, customerID)
	if err != nil {
		return err
	}

	uploader := manager.NewUploader(client)

	md := make(map[string]string)
	for k, v := range kv {
		md[k] = v
	}

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(cfg.S3Uploader.S3Bucket),
		Key:         aws.String(key),
		Body:        buf,
		ContentType: &contentType,
		Metadata:    md,
	})
	if err != nil {
		return err
	}

	return nil
}
