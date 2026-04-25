FROM golang:1.25 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/airflow-worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/airflow-worker /usr/local/bin/airflow-worker

ENTRYPOINT ["/usr/local/bin/airflow-worker"]
