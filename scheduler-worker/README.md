# Go, Echo, Templ, Tailwind Starter Template

A fast, minimal starter template for building server-rendered web applications in Go using Echo, Templ, TailwindCSS, and Templ UI.

This project gives you a solid foundation to build from — with preconfigured defaults, an opinionated folder structure, and server-rendered HTML out of the box.

---

## Features

- Fast startup, zero config needed
- Opinionated folder structure with `/cmd` and `/internal`
- Templ components using [`templ`](https://templ.guide)
- Utility-first styling with TailwindCSS
- Templ UI support preconfigured (All components are installed)
- Clean routing with Echo (My favorite Go web framework)
- OpenTelemetry tracing middleware for all routes
- Centralized error handling
- Graceful shutdown
- Makefile-driven dev workflow

---

## Tool Links

- **[Echo](https://echo.labstack.com/)**: A high-performance, minimalist web framework for Go.
- **[Templ](https://templ.guide/)**: A Go HTML templating engine that allows you to build reusable components.
- **[TailwindCSS](https://tailwindcss.com/)**: A utility-first CSS framework for rapid UI development.
- **[Templ UI](https://templui.io/)**: A collection of prebuilt components for Templ, making it easy to build beautiful UIs.
- **[Air](https://github.com/air-verse/air)**: A live reloading tool for Go applications, making development faster and smoother.


## Project Structure

``` If you're actually looking at this, message me if you do anything cool with this template! xD
├── cmd/                # Entrypoint (main.go)
├── internal/           # Application logic
│   ├── handlers/       # HTTP handlers
│   │   ├── errors/     # Centralized error response logic
│   │   └── v1/         # Versioned routing
│   ├── httpserver/     # Server wiring & middleware
│   └── observability/  # OTEL setup + span-aware logging helper
├── .templui.json       # Templ UI config
├── Makefile            # Dev commands
```

---

## Get Started

### 1. Clone the repo

```bash
git clone https://github.com/brian-nunez/scheduler-worker.git
cd scheduler-worker
```

### 2. Install dependencies

* Go 1.22+
* templ
* tailwindcss
* air (for live reloading)

### 3. Run in dev mode

```bash
make dev
```

## OpenTelemetry (Simple Setup)

Tracing is enabled for all routes by one Echo middleware in `internal/httpserver`.
Logs are exported over OTLP/gRPC and can be emitted with:
- `observability.LogDebug(ctx, "...")`
- `observability.LogInfo(ctx, "...")`
- `observability.LogWarn(ctx, "...")`
- `observability.LogError(ctx, "...")`

Environment variables:

```bash
OTEL_ENABLED=true
OTEL_SERVICE_NAME=scheduler-worker
OTEL_SERVICE_VERSION=
OTEL_ENVIRONMENT=development
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
OTEL_EXPORTER_OTLP_INSECURE=true
OTEL_FAIL_FAST=false
```

Local LGTM:

```bash
docker run --rm -p 3000:3000 -p 4317:4317 -p 4318:4318 grafana/otel-lgtm:latest
```

Span-aware log helpers:

```go
observability.LogInfo(c.Request().Context(), "health_check: service is running")
observability.LogWarn(c.Request().Context(), "degraded_dependency", "dependency", "redis")
observability.LogError(c.Request().Context(), "failed_to_process", "error", err.Error())
```

## Reach out if you have questions or just want to chat!

- [GitHub](https://www.github.com/brian-nunez)
- [LinkedIn](https://www.linkedin.com/in/brianjnunez)
