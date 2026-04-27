package worker

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultPostgresDSN           = "postgres://scanning:scanning@postgres:5432/scanning?sslmode=disable"
	defaultRedisAddr             = "redis:6379"
	defaultRedisDB               = 0
	defaultConsumerGroup         = "scan-workers"
	defaultPollInterval          = 5 * time.Second
	defaultRecoveryInterval      = 1 * time.Minute
	defaultClaimLease            = 2 * time.Minute
	defaultClaimBatchSize        = 100
	defaultMaxQueueDepth         = int64(5000)
	defaultStreamNormal          = "scan_jobs:normal"
	defaultStreamUrgent          = "scan_jobs:urgent"
	defaultPriorityUrgentMinimum = 50
)

type Config struct {
	PostgresDSN             string
	RedisAddr               string
	RedisPassword           string
	RedisDB                 int
	ConsumerGroup           string
	PollInterval            time.Duration
	RecoveryInterval        time.Duration
	ClaimLease              time.Duration
	ClaimBatchSize          int
	MaxQueueDepth           int64
	StreamNormal            string
	StreamUrgent            string
	PriorityUrgentThreshold int
	WorkerID                string
	MaxAttempts             int
}

func LoadConfigFromEnv() (Config, error) {
	host, _ := os.Hostname()
	if host == "" {
		host = "scheduler-worker"
	}

	cfg := Config{
		PostgresDSN:             envOrDefault("POSTGRES_DSN", defaultPostgresDSN),
		RedisAddr:               envOrDefault("REDIS_ADDR", defaultRedisAddr),
		RedisPassword:           envOrDefault("REDIS_PASSWORD", ""),
		ConsumerGroup:           envOrDefault("REDIS_CONSUMER_GROUP", defaultConsumerGroup),
		PollInterval:            durationEnvOrDefault("SCHEDULER_POLL_INTERVAL", defaultPollInterval),
		RecoveryInterval:        durationEnvOrDefault("SCHEDULER_RECOVERY_INTERVAL", defaultRecoveryInterval),
		ClaimLease:              durationEnvOrDefault("SCHEDULER_CLAIM_LEASE", defaultClaimLease),
		StreamNormal:            envOrDefault("REDIS_STREAM_NORMAL", defaultStreamNormal),
		StreamUrgent:            envOrDefault("REDIS_STREAM_URGENT", defaultStreamUrgent),
		WorkerID:                envOrDefault("SCHEDULER_WORKER_ID", host),
		MaxAttempts:             intEnvOrDefault("SCAN_MAX_ATTEMPTS", 3),
		RedisDB:                 intEnvOrDefault("REDIS_DB", defaultRedisDB),
		ClaimBatchSize:          intEnvOrDefault("SCHEDULER_CLAIM_BATCH_SIZE", defaultClaimBatchSize),
		PriorityUrgentThreshold: intEnvOrDefault("PRIORITY_URGENT_THRESHOLD", defaultPriorityUrgentMinimum),
		MaxQueueDepth:           int64EnvOrDefault("SCHEDULER_MAX_QUEUE_DEPTH", defaultMaxQueueDepth),
	}

	if cfg.ClaimBatchSize <= 0 {
		return Config{}, fmt.Errorf("SCHEDULER_CLAIM_BATCH_SIZE must be > 0")
	}
	if cfg.MaxQueueDepth <= 0 {
		return Config{}, fmt.Errorf("SCHEDULER_MAX_QUEUE_DEPTH must be > 0")
	}
	if cfg.MaxAttempts <= 0 {
		return Config{}, fmt.Errorf("SCAN_MAX_ATTEMPTS must be > 0")
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intEnvOrDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}

	return parsed
}

func int64EnvOrDefault(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}

	return parsed
}

func durationEnvOrDefault(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}

	return parsed
}
