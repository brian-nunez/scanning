package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	logglobal "go.opentelemetry.io/otel/log/global"
)

const defaultLogScope = "github.com/brian-nunez/scheduler-worker"

var otelLogger = slog.New(otelslog.NewHandler(
	defaultLogScope,
	otelslog.WithLoggerProvider(logglobal.GetLoggerProvider()),
))

func LogDebug(ctx context.Context, message string, attrs ...any) {
	otelLogger.DebugContext(ctx, message, attrs...)
}

func LogInfo(ctx context.Context, message string, attrs ...any) {
	otelLogger.InfoContext(ctx, message, attrs...)
}

func LogWarn(ctx context.Context, message string, attrs ...any) {
	otelLogger.WarnContext(ctx, message, attrs...)
}

func LogError(ctx context.Context, message string, attrs ...any) {
	otelLogger.ErrorContext(ctx, message, attrs...)
}
