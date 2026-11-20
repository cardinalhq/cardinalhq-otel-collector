// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package googlecloudpubsubreceiver

import (
	"context"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cardinalhq/cardinalhq-otel-collector/receiver/googlecloudpubsubreceiver/internal/metadata"
)

// twoRecordUnmarshaler decodes any payload into two log records, so the test
// checks that metadata lands on every record of a message.
type twoRecordUnmarshaler struct{}

func (twoRecordUnmarshaler) UnmarshalLogs(buf []byte) (plog.Logs, error) {
	out := plog.NewLogs()
	lrs := out.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	lrs.AppendEmpty().Body().SetStr(string(buf))
	lrs.AppendEmpty().Body().SetStr(string(buf))
	return out, nil
}

func newMetadataTestReceiver(t *testing.T, include bool) (*pubsubReceiver, *consumertest.LogsSink) {
	t.Helper()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })
	recv, _ := createObservedReceiver(t, srv)
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.NewID(metadata.Type),
		Transport:              reportTransport,
		ReceiverCreateSettings: recv.settings,
	})
	require.NoError(t, err)
	recv.obsrecv = obsrecv
	sink := new(consumertest.LogsSink)
	recv.logsConsumer = sink
	recv.logsUnmarshaler = twoRecordUnmarshaler{}
	recv.config.IncludeMetadata = include
	return recv, sink
}

func TestHandleLogIncludeMetadata(t *testing.T) {
	recv, sink := newMetadataTestReceiver(t, true)
	publish := time.Date(2026, 11, 20, 21, 3, 4, 123456789, time.FixedZone("x", 3600))
	msg := &pb.ReceivedMessage{
		AckId:           "ack-1",
		DeliveryAttempt: 2,
		Message: &pb.PubsubMessage{
			MessageId:   "17368741234567890",
			Data:        []byte(`{"event_type":"x"}`),
			PublishTime: timestamppb.New(publish),
			OrderingKey: "k1",
			Attributes:  map[string]string{"b": "2", "a": "1"},
		},
	}
	require.NoError(t, recv.handleLog(context.Background(), msg, uncompressed))

	require.Len(t, sink.AllLogs(), 1)
	lrs := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	require.Equal(t, 2, lrs.Len())
	for i := 0; i < lrs.Len(); i++ {
		lr := lrs.At(i)
		attrs := lr.Attributes().AsRaw()
		assert.Equal(t, "17368741234567890", attrs[attrMessageID])
		assert.Equal(t, "projects/my-project/subscriptions/otlp", attrs[attrSubscription])
		// UTC, microsecond precision, fixed width.
		assert.Equal(t, "2026-11-20T20:03:04.123456Z", attrs[attrPublishTime])
		assert.Equal(t, "k1", attrs[attrOrderingKey])
		assert.Equal(t, int64(2), attrs[attrDeliveryAttempt])
		assert.Equal(t, map[string]any{"a": "1", "b": "2"}, attrs[attrAttributes])
		// The record timestamps stay as the encoder set them.
		assert.Zero(t, lr.Timestamp())
	}
}

func TestHandleLogIncludeMetadataOmitsEmpty(t *testing.T) {
	recv, sink := newMetadataTestReceiver(t, true)
	msg := &pb.ReceivedMessage{
		AckId:   "ack-2",
		Message: &pb.PubsubMessage{MessageId: "m2", Data: []byte("x")},
	}
	require.NoError(t, recv.handleLog(context.Background(), msg, uncompressed))
	attrs := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().AsRaw()
	assert.Equal(t, map[string]any{
		attrMessageID:    "m2",
		attrSubscription: "projects/my-project/subscriptions/otlp",
	}, attrs)
}

func TestHandleLogMetadataDisabledByDefault(t *testing.T) {
	recv, sink := newMetadataTestReceiver(t, false)
	msg := &pb.ReceivedMessage{
		AckId: "ack-3",
		Message: &pb.PubsubMessage{
			MessageId:  "m3",
			Data:       []byte("x"),
			Attributes: map[string]string{"a": "1"},
		},
	}
	require.NoError(t, recv.handleLog(context.Background(), msg, uncompressed))
	attrs := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	assert.Equal(t, 0, attrs.Len())
}
