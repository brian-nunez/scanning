FROM golang:1.25 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/scheduler-worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/scheduler-worker /usr/local/bin/scheduler-worker

ENTRYPOINT ["/usr/local/bin/scheduler-worker"]
