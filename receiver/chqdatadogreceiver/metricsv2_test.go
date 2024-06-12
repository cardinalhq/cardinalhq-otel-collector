package datadogreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestFilterOlderThan(t *testing.T) {
	ddp := &datadogReceiver{
		metricLogger: zap.NewNop(),
	}
	targetTime := time.Now()

	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	ilm := rm.ScopeMetrics().AppendEmpty()
	metric := ilm.Metrics().AppendEmpty()
	metric.SetName("test_metric")
	gauge := metric.SetEmptyGauge()
	gaugeDataPoints := gauge.DataPoints()
	dp1 := gaugeDataPoints.AppendEmpty()
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(targetTime.Add(-time.Minute)))
	dp2 := gaugeDataPoints.AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(targetTime.Add(-time.Second)))
	dp3 := gaugeDataPoints.AppendEmpty()
	dp3.SetTimestamp(pcommon.NewTimestampFromTime(targetTime.Add(time.Second)))

	keptDatapoints, removedDatapoints := ddp.filterOlderThan(&m, targetTime)

	assert.Equal(t, 1, keptDatapoints)
	assert.Equal(t, 2, removedDatapoints)
}
