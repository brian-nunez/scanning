package observability

import (
	"os"
	"strconv"
	"strings"
)

const defaultServiceName = "airflow-worker"

type Config struct {
	Enabled        bool
	ServiceName    string
	ServiceVersion string
	Environment    string
	OTLPEndpoint   string
	OTLPInsecure   bool
	FailFast       bool
}

func LoadConfigFromEnv() Config {
	return Config{
		Enabled:        envBool("OTEL_ENABLED", true),
		ServiceName:    envString("OTEL_SERVICE_NAME", defaultServiceName),
		ServiceVersion: envString("OTEL_SERVICE_VERSION", ""),
		Environment:    envString("OTEL_ENVIRONMENT", "development"),
		OTLPEndpoint:   envString("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		OTLPInsecure:   envBool("OTEL_EXPORTER_OTLP_INSECURE", true),
		FailFast:       envBool("OTEL_FAIL_FAST", false),
	}
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}

	return parsed
}
