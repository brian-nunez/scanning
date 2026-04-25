package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type recurringScan struct {
	ID                  string
	AssetID             string
	AssetType           string
	Frequency           string
	Priority            int
	NextRunAt           time.Time
	SpreadOffsetSeconds int
}

type Worker struct {
	cfg    Config
	logger *slog.Logger
	pg     *pgxpool.Pool
	redis  *redis.Client
}

func New(cfg Config, logger *slog.Logger) (Service, error) {
	if logger == nil {
		logger = slog.Default()
	}

	pg, err := pgxpool.New(context.Background(), cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	return &Worker{
		cfg:    cfg,
		logger: logger,
		pg:     pg,
		redis:  rdb,
	}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	if err := w.ping(ctx); err != nil {
		return err
	}

	pollTicker := time.NewTicker(w.cfg.PollInterval)
	defer pollTicker.Stop()

	recoveryTicker := time.NewTicker(w.cfg.RecoveryInterval)
	defer recoveryTicker.Stop()

	w.logger.Info("scheduler worker started",
		"poll_interval", w.cfg.PollInterval,
		"recovery_interval", w.cfg.RecoveryInterval,
		"claim_batch_size", w.cfg.ClaimBatchSize,
		"max_queue_depth", w.cfg.MaxQueueDepth,
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-recoveryTicker.C:
			if err := w.recoverExpiredClaims(ctx); err != nil {
				w.logger.Error("recovery loop failed", "error", err)
			}
		case <-pollTicker.C:
			if err := w.runOnce(ctx); err != nil {
				w.logger.Error("scheduler iteration failed", "error", err)
			}
		}
	}
}

func (w *Worker) Stop(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.pg.Close()
		_ = w.redis.Close()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := w.pg.Ping(pingCtx); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	if err := w.redis.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("redis ping failed: %w", err)
	}

	return nil
}

func (w *Worker) runOnce(ctx context.Context) error {
	depth, err := w.queueDepth(ctx)
	if err != nil {
		return err
	}

	if depth >= w.cfg.MaxQueueDepth {
		w.logger.Warn("scheduler backpressure active",
			"queue_depth", depth,
			"max_queue_depth", w.cfg.MaxQueueDepth,
		)
		return nil
	}

	scans, err := w.claimDueScans(ctx)
	if err != nil {
		return err
	}
	if len(scans) == 0 {
		return nil
	}

	w.logger.Info("claimed recurring scans", "count", len(scans))

	for _, scan := range scans {
		if err := w.processClaimedScan(ctx, scan); err != nil {
			w.logger.Error("failed to enqueue claimed scan", "scan_id", scan.ID, "error", err)
		}
	}

	return nil
}

func (w *Worker) queueDepth(ctx context.Context) (int64, error) {
	normalDepth, err := w.redis.XLen(ctx, w.cfg.StreamNormal).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("get normal stream depth: %w", err)
	}

	urgentDepth, err := w.redis.XLen(ctx, w.cfg.StreamUrgent).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("get urgent stream depth: %w", err)
	}

	return normalDepth + urgentDepth, nil
}

func (w *Worker) claimDueScans(ctx context.Context) ([]recurringScan, error) {
	query := `
WITH due AS (
    SELECT id
    FROM recurring_scans
    WHERE enabled = true
      AND status = 'scheduled'
      AND next_run_at <= now()
    ORDER BY priority DESC, next_run_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
)
UPDATE recurring_scans rs
SET status = 'claiming',
    claim_owner = $1,
    claimed_at = now(),
    claim_expires_at = now() + $3::interval,
    updated_at = now()
FROM due
WHERE rs.id = due.id
RETURNING rs.id::text, rs.asset_id::text, rs.asset_type, rs.frequency, rs.priority, rs.next_run_at, rs.spread_offset_seconds;
`

	interval := fmt.Sprintf("%d seconds", int(w.cfg.ClaimLease.Seconds()))
	rows, err := w.pg.Query(ctx, query, w.cfg.WorkerID, w.cfg.ClaimBatchSize, interval)
	if err != nil {
		return nil, fmt.Errorf("claim due scans: %w", err)
	}
	defer rows.Close()

	scans := make([]recurringScan, 0, w.cfg.ClaimBatchSize)
	for rows.Next() {
		var scan recurringScan
		if err := rows.Scan(
			&scan.ID,
			&scan.AssetID,
			&scan.AssetType,
			&scan.Frequency,
			&scan.Priority,
			&scan.NextRunAt,
			&scan.SpreadOffsetSeconds,
		); err != nil {
			return nil, fmt.Errorf("scan claimed row: %w", err)
		}

		scans = append(scans, scan)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed rows: %w", err)
	}

	return scans, nil
}

func (w *Worker) processClaimedScan(ctx context.Context, scan recurringScan) error {
	scheduledFor := scan.NextRunAt.UTC()
	redisJobKey := fmt.Sprintf("scan:%s:%d", scan.ID, scheduledFor.Unix())

	runID, err := w.ensureRunRow(ctx, scan, scheduledFor, redisJobKey)
	if err != nil {
		return err
	}

	dedupeKey := fmt.Sprintf("dedupe:%s", redisJobKey)
	created, err := w.redis.SetNX(ctx, dedupeKey, "1", w.cfg.RedisDedupeTTL).Result()
	if err != nil {
		return fmt.Errorf("set redis dedupe key: %w", err)
	}

	nextRunAt, spreadOffset, err := NextRunAtForFrequency(scan.AssetID, scan.Frequency, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("calculate next_run_at: %w", err)
	}

	if !created {
		w.logger.Info("dedupe key already exists, skipping redis enqueue",
			"scan_id", scan.ID,
			"redis_job_key", redisJobKey,
		)

		if err := w.advanceSchedule(ctx, scan.ID, nextRunAt, spreadOffset); err != nil {
			return fmt.Errorf("advance schedule after dedupe skip: %w", err)
		}

		return nil
	}

	stream := w.cfg.StreamNormal
	if scan.Priority >= w.cfg.PriorityUrgentThreshold {
		stream = w.cfg.StreamUrgent
	}

	payload := map[string]any{
		"scan_run_id":       runID,
		"recurring_scan_id": scan.ID,
		"asset_id":          scan.AssetID,
		"asset_type":        scan.AssetType,
		"priority":          scan.Priority,
		"scheduled_for":     scheduledFor.Format(time.RFC3339Nano),
		"frequency":         strings.ToLower(scan.Frequency),
	}

	redisMessageID, err := w.redis.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: payload}).Result()
	if err != nil {
		// Roll back dedupe guard on enqueue failure so this run can be retried.
		_ = w.redis.Del(ctx, dedupeKey).Err()
		return fmt.Errorf("enqueue redis stream message: %w", err)
	}

	if err := w.markRunEnqueued(ctx, runID, stream, redisMessageID); err != nil {
		return err
	}

	if err := w.advanceSchedule(ctx, scan.ID, nextRunAt, spreadOffset); err != nil {
		return err
	}

	encodedPayload, _ := json.Marshal(payload)
	w.logger.Info("enqueued scan run",
		"scan_run_id", runID,
		"stream", stream,
		"redis_message_id", redisMessageID,
		"payload", string(encodedPayload),
	)

	return nil
}

func (w *Worker) ensureRunRow(ctx context.Context, scan recurringScan, scheduledFor time.Time, redisJobKey string) (string, error) {
	runID := uuid.New().String()

	query := `
INSERT INTO recurring_scan_runs (
    id,
    recurring_scan_id,
    asset_id,
    asset_type,
    scheduled_for,
    status,
    priority,
    redis_job_key,
    max_attempts,
    created_at,
    updated_at
)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, 'created', $6, $7, $8, now(), now())
ON CONFLICT (redis_job_key)
DO UPDATE SET updated_at = now()
RETURNING id::text;
`

	var existingRunID string
	err := w.pg.QueryRow(ctx, query,
		runID,
		scan.ID,
		scan.AssetID,
		scan.AssetType,
		scheduledFor,
		scan.Priority,
		redisJobKey,
		w.cfg.MaxAttempts,
	).Scan(&existingRunID)
	if err != nil {
		return "", fmt.Errorf("insert or fetch scan run row: %w", err)
	}

	return existingRunID, nil
}

func (w *Worker) markRunEnqueued(ctx context.Context, runID, stream, redisMessageID string) error {
	query := `
UPDATE recurring_scan_runs
SET status = 'enqueued',
    redis_stream = $2,
    redis_message_id = $3,
    updated_at = now()
WHERE id = $1::uuid;
`

	cmd, err := w.pg.Exec(ctx, query, runID, stream, redisMessageID)
	if err != nil {
		return fmt.Errorf("mark run enqueued: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("mark run enqueued: run %s not found", runID)
	}

	return nil
}

func (w *Worker) advanceSchedule(ctx context.Context, scanID string, nextRunAt time.Time, spreadOffset int64) error {
	query := `
UPDATE recurring_scans
SET status = 'scheduled',
    last_run_at = now(),
    next_run_at = $2,
    spread_offset_seconds = $3,
    claim_owner = NULL,
    claimed_at = NULL,
    claim_expires_at = NULL,
    updated_at = now()
WHERE id = $1::uuid;
`

	cmd, err := w.pg.Exec(ctx, query, scanID, nextRunAt, spreadOffset)
	if err != nil {
		return fmt.Errorf("advance recurring scan schedule: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("advance recurring scan schedule: scan %s not found", scanID)
	}

	return nil
}

func (w *Worker) recoverExpiredClaims(ctx context.Context) error {
	query := `
UPDATE recurring_scans
SET status = 'scheduled',
    claim_owner = NULL,
    claimed_at = NULL,
    claim_expires_at = NULL,
    updated_at = now()
WHERE status = 'claiming'
  AND claim_expires_at < now();
`

	cmd, err := w.pg.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("recover expired claims: %w", err)
	}

	if cmd.RowsAffected() > 0 {
		w.logger.Warn("recovered expired claims", "count", cmd.RowsAffected())
	}

	return nil
}
