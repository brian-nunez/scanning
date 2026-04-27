package worker

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultPostgresDSN          = "postgres://scanning:scanning@postgres:5432/scanning?sslmode=disable"
	defaultRedisAddr            = "redis:6379"
	defaultRedisDB              = 0
	defaultStreamNormal         = "scan_jobs:normal"
	defaultStreamUrgent         = "scan_jobs:urgent"
	defaultConsumerGroup        = "scan-workers"
	defaultWorkersPerController = 4
	defaultReadCount            = 1
	defaultReadBlock            = 5 * time.Second
	defaultUrgentBurst          = 20
	defaultAirflowBaseURL       = "http://airflow-webserver:8080"
	defaultAirflowDAGID         = "scan_example"
	defaultAirflowTaskID        = "run_scan"
	defaultAirflowPollInterval  = 3 * time.Second
	defaultAirflowRunTimeout    = 5 * time.Minute
	defaultPELRecoveryInterval  = 30 * time.Second
	defaultPELRecoveryCount     = 100
)

type Config struct {
	PostgresDSN          string
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	StreamNormal         string
	StreamUrgent         string
	ConsumerGroup        string
	ConsumerName         string
	WorkersPerController int
	ReadCount            int64
	ReadBlock            time.Duration
	UrgentBurst          int
	AirflowBaseURL       string
	AirflowDAGID         string
	AirflowTaskID        string
	AirflowUsername      string
	AirflowPassword      string
	AirflowPollInterval  time.Duration
	AirflowRunTimeout    time.Duration
	PELRecoveryInterval  time.Duration
	PELMinIdle           time.Duration
	PELRecoveryCount     int64
}

func LoadConfigFromEnv() (Config, error) {
	host, _ := os.Hostname()
	if host == "" {
		host = "airflow-worker"
	}

	cfg := Config{
		PostgresDSN:          envOrDefault("POSTGRES_DSN", defaultPostgresDSN),
		RedisAddr:            envOrDefault("REDIS_ADDR", defaultRedisAddr),
		RedisPassword:        envOrDefault("REDIS_PASSWORD", ""),
		RedisDB:              intEnvOrDefault("REDIS_DB", defaultRedisDB),
		StreamNormal:         envOrDefault("REDIS_STREAM_NORMAL", defaultStreamNormal),
		StreamUrgent:         envOrDefault("REDIS_STREAM_URGENT", defaultStreamUrgent),
		ConsumerGroup:        envOrDefault("REDIS_CONSUMER_GROUP", defaultConsumerGroup),
		ConsumerName:         envOrDefault("REDIS_CONSUMER_NAME", host),
		WorkersPerController: intEnvOrDefault("WORKERS_PER_CONTROLLER", defaultWorkersPerController),
		ReadCount:            int64(intEnvOrDefault("REDIS_READ_COUNT", defaultReadCount)),
		ReadBlock:            durationEnvOrDefault("REDIS_READ_BLOCK", defaultReadBlock),
		UrgentBurst:          intEnvOrDefault("REDIS_URGENT_BURST", defaultUrgentBurst),
		AirflowBaseURL:       envOrDefault("AIRFLOW_BASE_URL", defaultAirflowBaseURL),
		AirflowDAGID:         envOrDefault("AIRFLOW_DAG_ID", defaultAirflowDAGID),
		AirflowTaskID:        envOrDefault("AIRFLOW_TASK_ID", defaultAirflowTaskID),
		AirflowUsername:      envOrDefault("AIRFLOW_USERNAME", "admin"),
		AirflowPassword:      envOrDefault("AIRFLOW_PASSWORD", "admin"),
		AirflowPollInterval:  durationEnvOrDefault("AIRFLOW_POLL_INTERVAL", defaultAirflowPollInterval),
		AirflowRunTimeout:    durationEnvOrDefault("AIRFLOW_RUN_TIMEOUT", defaultAirflowRunTimeout),
		PELRecoveryInterval:  durationEnvOrDefault("REDIS_PEL_RECOVERY_INTERVAL", defaultPELRecoveryInterval),
		PELMinIdle:           durationEnvOrDefault("REDIS_PEL_MIN_IDLE", defaultAirflowRunTimeout),
		PELRecoveryCount:     int64(intEnvOrDefault("REDIS_PEL_RECOVERY_COUNT", defaultPELRecoveryCount)),
	}

	if cfg.WorkersPerController <= 0 {
		return Config{}, fmt.Errorf("WORKERS_PER_CONTROLLER must be > 0")
	}
	if cfg.ReadCount <= 0 {
		return Config{}, fmt.Errorf("REDIS_READ_COUNT must be > 0")
	}
	if cfg.UrgentBurst <= 0 {
		return Config{}, fmt.Errorf("REDIS_URGENT_BURST must be > 0")
	}
	if cfg.PELRecoveryInterval <= 0 {
		return Config{}, fmt.Errorf("REDIS_PEL_RECOVERY_INTERVAL must be > 0")
	}
	if cfg.PELMinIdle <= 0 {
		return Config{}, fmt.Errorf("REDIS_PEL_MIN_IDLE must be > 0")
	}
	if cfg.PELRecoveryCount <= 0 {
		return Config{}, fmt.Errorf("REDIS_PEL_RECOVERY_COUNT must be > 0")
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
