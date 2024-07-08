package translate

import (
	"log/slog"
	"os"
	"strings"
	"sync"
)

const (
	CardinalEnvCustomerID  = "CARDINALHQ_CUSTOMER_ID"
	CardinalEnvCollectorID = "CARDINALHQ_COLLECTOR_ID"
)

type Environment struct {
	tags map[string]string
}

var (
	environmentSetupOnce sync.Once
	environment          *Environment
)

func EnvironmentFromEnv() *Environment {
	environmentSetupOnce.Do(func() {
		environment = environmentFromEnv()
	})
	return environment
}

func environmentFromEnv() *Environment {
	customerid := os.Getenv(CardinalEnvCustomerID)
	collectorid := os.Getenv(CardinalEnvCollectorID)

	tags := make(map[string]string)
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "CARDINALHQ_ENV_") {
			parts := strings.SplitN(v, "=", 2)
			if len(parts) == 2 {
				tagname := strings.TrimPrefix(parts[0], "CARDINALHQ_ENV_")
				tagname = strings.ToLower(tagname)
				tags[tagname] = parts[1]
			}
		}
	}

	tags["customer_id"] = customerid
	tags["collector_id"] = collectorid

	slog.Info("Environment tags", slog.Any("tags", tags))

	return &Environment{
		tags: tags,
	}
}

func (e *Environment) CustomerID() string {
	return e.tags["customer_id"]
}

func (e *Environment) CollectorID() string {
	return e.tags["collector_id"]
}

func (e *Environment) Tags() map[string]string {
	return e.tags
}
