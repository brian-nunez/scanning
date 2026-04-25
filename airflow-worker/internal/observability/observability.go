package observability

import (
	"context"
	"errors"
	"fmt"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type Telemetry struct {
	shutdownFns []func(context.Context) error
}

func Init(ctx context.Context, config Config, logger *log.Logger) (*Telemetry, error) {
	if logger == nil {
		logger = log.Default()
	}

	telemetry := &Telemetry{shutdownFns: make([]func(context.Context) error, 0, 2)}
	if !config.Enabled {
		logger.Println("observability: tracing disabled")
		return telemetry, nil
	}

	res, err := buildResource(ctx, config)
	if err != nil {
		if config.FailFast {
			return nil, fmt.Errorf("build OTEL resource: %w", err)
		}

		logger.Printf("observability: resource error, using defaults: %v", err)
		res = resource.Default()
	}

	traceProvider, err := newTracerProvider(ctx, config, res)
	if err != nil {
		if config.FailFast {
			return nil, fmt.Errorf("initialize OTEL tracing: %w", err)
		}

		logger.Printf("observability: tracing disabled (init error): %v", err)
		return telemetry, nil
	}

	otel.SetTracerProvider(traceProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	telemetry.shutdownFns = append(telemetry.shutdownFns, traceProvider.Shutdown)

	loggerProvider, err := newLoggerProvider(ctx, config, res)
	if err != nil {
		if config.FailFast {
			return nil, fmt.Errorf("initialize OTEL logs: %w", err)
		}

		logger.Printf("observability: logs disabled (init error): %v", err)
		return telemetry, nil
	}

	logglobal.SetLoggerProvider(loggerProvider)
	telemetry.shutdownFns = append(telemetry.shutdownFns, loggerProvider.Shutdown)

	return telemetry, nil
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	var shutdownErr error

	for index := len(t.shutdownFns) - 1; index >= 0; index-- {
		shutdownErr = errors.Join(shutdownErr, t.shutdownFns[index](ctx))
	}

	return shutdownErr
}

func buildResource(ctx context.Context, config Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(config.ServiceName),
		attribute.String("deployment.environment", config.Environment),
	}
	if config.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(config.ServiceVersion))
	}

	custom := resource.NewWithAttributes(semconv.SchemaURL, attrs...)
	return resource.Merge(resource.Default(), custom)
}

func newTracerProvider(ctx context.Context, config Config, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	options := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(config.OTLPEndpoint),
	}
	if config.OTLPInsecure {
		options = append(options, otlptracegrpc.WithInsecure())
	}

	traceExporter, err := otlptracegrpc.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	), nil
}

func newLoggerProvider(ctx context.Context, config Config, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	options := []otlploggrpc.Option{
		otlploggrpc.WithEndpoint(config.OTLPEndpoint),
	}
	if config.OTLPInsecure {
		options = append(options, otlploggrpc.WithInsecure())
	}

	logExporter, err := otlploggrpc.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
	), nil
}
