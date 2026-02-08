// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3TimeKey(t *testing.T) {
	const layout = "2006-01-02"

	tm, err := time.Parse(layout, "2022-06-05")
	timeKey := getTimeKey(tm, "hour", "")

	assert.NoError(t, err)
	require.NotNil(t, tm)
	assert.Equal(t, "year=2022/month=06/day=05/hour=00", timeKey)

	timeKey = getTimeKey(tm, "minute", "")
	assert.Equal(t, "year=2022/month=06/day=05/hour=00/minute=00", timeKey)
}

func TestS3Key(t *testing.T) {
	const layout = "2006-01-02"

	tm, err := time.Parse(layout, "2022-06-05")

	assert.NoError(t, err)
	require.NotNil(t, tm)

	re := regexp.MustCompile(`keyprefix/year=2022/month=06/day=05/hour=00/minute=00/fileprefixlogs_([0-9]+).json`)
	s3Key := getS3Key(tm, "keyprefix", "minute", "fileprefix", "logs", "json", "")
	matched := re.MatchString(s3Key)
	assert.Equal(t, true, matched)
}

func TestS3KeyEmptyFileFormat(t *testing.T) {
	const layout = "2006-01-02"

	tm, err := time.Parse(layout, "2022-06-05")

	assert.NoError(t, err)
	require.NotNil(t, tm)

	re := regexp.MustCompile(`keyprefix/year=2022/month=06/day=05/hour=00/minute=00/fileprefixlogs_([0-9]+)`)
	s3Key := getS3Key(tm, "keyprefix", "minute", "fileprefix", "logs", "", "")
	matched := re.MatchString(s3Key)
	assert.Equal(t, true, matched)
}

func TestGetS3ClientWithEndpoint(t *testing.T) {
	const endpoint = "https://endpoint.com"
	const region = "us-east-1"
	cfg := &Config{
		S3Uploader: S3UploaderConfig{
			Region:   region,
			Endpoint: endpoint,
		},
	}
	client, err := getS3Client(context.Background(), cfg, "")
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestGetS3ClientNoEndpoint(t *testing.T) {
	const region = "us-east-1"
	cfg := &Config{
		S3Uploader: S3UploaderConfig{
			Region: region,
		},
	}
	client, err := getS3Client(context.Background(), cfg, "")
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestGetS3ClientWithRoleArn(t *testing.T) {
	const region = "us-east-1"
	const roleArn = "arn:aws:iam::12345:role/s3-exportation-role"
	const customerID = "12345"
	cfg := &Config{
		S3Uploader: S3UploaderConfig{
			Region:  region,
			RoleArn: roleArn,
		},
	}

	client, err := getS3Client(context.Background(), cfg, customerID)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestGetS3ClientWithoutRoleArn(t *testing.T) {
	const region = "us-east-1"
	const customerID = "12345"
	cfg := &Config{
		S3Uploader: S3UploaderConfig{
			Region: region,
		},
	}

	client, err := getS3Client(context.Background(), cfg, customerID)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}
