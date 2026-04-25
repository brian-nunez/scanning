package httpserver

const defaultServiceName = "scheduler-worker"

type ObservabilityConfig struct {
	ServiceName    string
	TracingEnabled bool
}

func (c ObservabilityConfig) resolvedServiceName() string {
	if c.ServiceName == "" {
		return defaultServiceName
	}
	return c.ServiceName
}
