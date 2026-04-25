package httpserver

import (
	"context"

	v1 "github.com/brian-nunez/scheduler-worker/internal/handlers/v1"
)

type Server interface {
	Start(addr string) error
	Shutdown(ctx context.Context) error
}

type BootstrapConfig struct {
	StaticDirectories map[string]string
	Observability     ObservabilityConfig
}

func Bootstrap(config BootstrapConfig) Server {
	server := New().
		WithStaticAssets(config.StaticDirectories).
		WithDefaultMiddleware(config.Observability).
		WithErrorHandler().
		WithRoutes(v1.RegisterRoutes).
		WithNotFound().
		Build()

	return server
}
