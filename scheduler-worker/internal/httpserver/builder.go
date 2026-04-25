package httpserver

import (
	"github.com/brian-nunez/scheduler-worker/internal/handlers/errors"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type ServerBuilder struct {
	e *echo.Echo
}

func New() *ServerBuilder {
	return &ServerBuilder{
		e: echo.New(),
	}
}

func (b *ServerBuilder) WithDefaultMiddleware(config ObservabilityConfig) *ServerBuilder {
	b.e.Use(middleware.Recover())
	b.e.Use(middleware.RequestID())
	b.e.Use(middleware.CORS())

	if config.TracingEnabled {
		b.e.Use(otelecho.Middleware(
			config.resolvedServiceName(),
			otelecho.WithPropagators(otel.GetTextMapPropagator()),
			otelecho.WithTracerProvider(otel.GetTracerProvider()),
		))

		// Automatically expose trace correlation headers on all responses.
		b.e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
			return func(c echo.Context) error {
				spanContext := trace.SpanContextFromContext(c.Request().Context())
				if spanContext.IsValid() {
					c.Response().Header().Set("X-Trace-ID", spanContext.TraceID().String())
					c.Response().Header().Set("X-Span-ID", spanContext.SpanID().String())
				}

				return next(c)
			}
		})
	}

	b.e.Use(middleware.Logger())

	return b
}

func (b *ServerBuilder) WithRoutes(register func(e *echo.Echo)) *ServerBuilder {
	register(b.e)
	return b
}

func (b *ServerBuilder) WithErrorHandler() *ServerBuilder {
	b.e.HTTPErrorHandler = func(err error, c echo.Context) {
		code := echo.ErrInternalServerError.Code

		if he, ok := err.(*echo.HTTPError); ok {
			code = he.Code
		}

		c.Logger().Error(err)

		if !c.Response().Committed {
			response := errors.GenerateByStatusCode(code).Build()
			_ = c.JSON(response.HTTPStatusCode, response)
		}
	}

	return b
}

func (b *ServerBuilder) WithNotFound() *ServerBuilder {
	notFound := func(c echo.Context) error {
		response := errors.NotFound().Build()
		return c.JSON(response.HTTPStatusCode, response)
	}
	b.e.RouteNotFound("*", notFound)
	b.e.RouteNotFound("/*", notFound)

	return b
}

func (b *ServerBuilder) WithStaticAssets(directories map[string]string) *ServerBuilder {
	for path, dir := range directories {
		if dir == "" {

			continue
		}

		b.e.Static(path, dir)
	}

	return b
}

func (b *ServerBuilder) Build() *echo.Echo {
	return b.e
}
