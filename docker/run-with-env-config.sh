#!/busybox/sh

# Dynamic configuration startup script for CardinalHQ OpenTelemetry Collector
# This script handles dynamic configuration via CHQ_COLLECTOR_CONFIG_YAML environment variable
# Suitable for ECS, Cloud Run, or any environment that provides config via environment variables

set -e

# Check if CHQ_COLLECTOR_CONFIG_YAML environment variable is set
if [ -z "${CHQ_COLLECTOR_CONFIG_YAML}" ]; then
    echo "Error: CHQ_COLLECTOR_CONFIG_YAML environment variable is required"
    echo "This script is designed for deployments with dynamic configuration via environment variables"
    exit 1
fi

# Create a temporary config file
CONFIG_FILE="/tmp/collector-config.yaml"

echo "Writing dynamic configuration to ${CONFIG_FILE}"

# Write the config from environment variable
echo "${CHQ_COLLECTOR_CONFIG_YAML}" > "${CONFIG_FILE}"

echo "Configuration written successfully"

# Start the collector with the dynamic config, passing through any additional arguments
exec /app/bin/cardinalhq-otel-collector --config="${CONFIG_FILE}" "$@"