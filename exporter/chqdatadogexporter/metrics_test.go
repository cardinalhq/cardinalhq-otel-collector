package chqdatadogexporter

import (
	"testing"

	"github.com/tj/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestValueAsFloat64(t *testing.T) {
	pi := pmetric.NewNumberDataPoint()
	pi.SetDoubleValue(3.14)
	assert.Equal(t, 3.14, valueAsFloat64(pi))

	meaningoflife := pmetric.NewNumberDataPoint()
	meaningoflife.SetIntValue(42)
	assert.Equal(t, 42.0, valueAsFloat64(meaningoflife))
}
