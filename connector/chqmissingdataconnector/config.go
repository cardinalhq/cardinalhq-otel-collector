package chqmissingdataconnector

import (
	"fmt"
	"time"
)

var (
	defaultMaximumAge = 24 * time.Hour
	defaultInterval   = 1 * time.Minute
)

type Config struct {
	MaximumAge time.Duration `mapstructure:"maximum_age"`
	Interval   time.Duration `mapstructure:"interval"`
}

func (c *Config) Validate() error {
	if c.MaximumAge <= 1*time.Minute {
		return fmt.Errorf("maximum_age must be greater than 1 minute")
	}
	if c.Interval <= 1*time.Second {
		return fmt.Errorf("interval must be greater than 1 second")
	}
	return nil
}
