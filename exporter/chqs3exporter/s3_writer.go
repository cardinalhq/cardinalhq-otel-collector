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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3Writer struct {
}

var _ dataWriter = (*s3Writer)(nil)

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

func getSessionConfig(config *Config) *aws.Config {
	sessionConfig := &aws.Config{
		Region:           aws.String(config.S3Uploader.Region),
		S3ForcePathStyle: &config.S3Uploader.S3ForcePathStyle,
		DisableSSL:       &config.S3Uploader.DisableSSL,
	}

	endpoint := config.S3Uploader.Endpoint
	if endpoint != "" {
		sessionConfig.Endpoint = aws.String(endpoint)
	}

	return sessionConfig
}

func getSession(config *Config, sessionConfig *aws.Config) (*session.Session, error) {
	sess, err := session.NewSession(sessionConfig)

	if config.S3Uploader.RoleArn != "" {
		credentials := stscreds.NewCredentials(sess, config.S3Uploader.RoleArn)
		sess.Config.Credentials = credentials
	}

	return sess, err
}

func (s3writer *s3Writer) writeBuffer(_ context.Context, now time.Time, buf io.Reader, config *Config, metadata string, format string, kv map[string]string, customerID string) error {
	key := getS3Key(now,
		config.S3Uploader.S3Prefix, config.S3Uploader.S3Partition,
		config.S3Uploader.FilePrefix, metadata, format, customerID)

	contentType := ""
	if format == "parquet" {
		contentType = "application/vnd.apache.parquet"
	}

	sessionConfig := getSessionConfig(config)
	sess, err := getSession(config, sessionConfig)

	if err != nil {
		return err
	}

	uploader := s3manager.NewUploader(sess)

	md := make(map[string]*string)
	for k, v := range kv {
		md[k] = aws.String(v)
	}

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(config.S3Uploader.S3Bucket),
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
