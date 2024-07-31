// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0 language governing permissions and
// limitations under the License.

package datadogreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver"

import (
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"`
	// ReadTimeout of the http server
	ReadTimeout           time.Duration `mapstructure:"read_timeout"`
	MaxMetricDatapointAge time.Duration `mapstructure:"max_metric_datapoint_age"`
	.breakme

}

var _ component.Config = (*Config)(nil)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.MaxMetricDatapointAge <= 0 {
		cfg.MaxMetricDatapointAge = 10 * time.Minute
	}
	return nil
}
