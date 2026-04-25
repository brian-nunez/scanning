package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type streamMessage struct {
	Stream string
	Msg    redis.XMessage
}

type Worker struct {
	cfg          Config
	logger       *slog.Logger
	pg           *pgxpool.Pool
	redis        *redis.Client
	httpClient   *http.Client
	workerGroup  sync.WaitGroup
	stopOnce     sync.Once
	started      bool
	startedMutex sync.Mutex
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
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	if err := w.ping(ctx); err != nil {
		return err
	}

	if err := w.ensureConsumerGroup(ctx, w.cfg.StreamUrgent); err != nil {
		return err
	}
	if err := w.ensureConsumerGroup(ctx, w.cfg.StreamNormal); err != nil {
		return err
	}

	w.startedMutex.Lock()
	if !w.started {
		for i := 0; i < w.cfg.WorkersPerController; i++ {
			workerID := i + 1
			w.workerGroup.Add(1)
			go w.localWorkerLoop(ctx, workerID)
		}
		w.started = true
	}
	w.startedMutex.Unlock()

	w.logger.Info("airflow worker started",
		"workers_per_controller", w.cfg.WorkersPerController,
		"consumer_group", w.cfg.ConsumerGroup,
		"consumer", w.cfg.ConsumerName,
		"urgent_stream", w.cfg.StreamUrgent,
		"normal_stream", w.cfg.StreamNormal,
	)

	<-ctx.Done()
	w.workerGroup.Wait()

	return nil
}

func (w *Worker) Stop(ctx context.Context) error {
	done := make(chan struct{})

	w.stopOnce.Do(func() {
		go func() {
			defer close(done)
			w.pg.Close()
			_ = w.redis.Close()
		}()
	})

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

func (w *Worker) ensureConsumerGroup(ctx context.Context, stream string) error {
	err := w.redis.XGroupCreateMkStream(ctx, stream, w.cfg.ConsumerGroup, "0").Err()
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}

	return fmt.Errorf("create consumer group for stream %s: %w", stream, err)
}

func (w *Worker) localWorkerLoop(ctx context.Context, workerID int) {
	defer w.workerGroup.Done()

	urgentProcessed := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := w.readNextMessage(ctx, urgentProcessed)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			w.logger.Error("failed reading redis stream", "worker_id", workerID, "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if msg == nil {
			continue
		}

		if msg.Stream == w.cfg.StreamUrgent {
			urgentProcessed++
		} else {
			urgentProcessed = 0
		}

		if urgentProcessed >= w.cfg.UrgentBurst {
			urgentProcessed = 0
		}

		if err := w.processMessage(ctx, *msg); err != nil {
			w.logger.Error("failed processing message", "worker_id", workerID, "stream", msg.Stream, "message_id", msg.Msg.ID, "error", err)
		}
	}
}

func (w *Worker) readNextMessage(ctx context.Context, urgentProcessed int) (*streamMessage, error) {
	if urgentProcessed < w.cfg.UrgentBurst {
		if msg, err := w.readFromStream(ctx, w.cfg.StreamUrgent, 250*time.Millisecond); err != nil {
			return nil, err
		} else if msg != nil {
			return msg, nil
		}
	}

	if msg, err := w.readFromStream(ctx, w.cfg.StreamNormal, 250*time.Millisecond); err != nil {
		return nil, err
	} else if msg != nil {
		return msg, nil
	}

	return w.readFromStreams(ctx, []string{w.cfg.StreamUrgent, w.cfg.StreamNormal}, w.cfg.ReadBlock)
}

func (w *Worker) readFromStream(ctx context.Context, stream string, block time.Duration) (*streamMessage, error) {
	results, err := w.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    w.cfg.ConsumerGroup,
		Consumer: w.cfg.ConsumerName,
		Streams:  []string{stream, ">"},
		Count:    w.cfg.ReadCount,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	if len(results) == 0 || len(results[0].Messages) == 0 {
		return nil, nil
	}

	return &streamMessage{Stream: stream, Msg: results[0].Messages[0]}, nil
}

func (w *Worker) readFromStreams(ctx context.Context, streams []string, block time.Duration) (*streamMessage, error) {
	streamArgs := make([]string, 0, len(streams)*2)
	for _, stream := range streams {
		streamArgs = append(streamArgs, stream)
	}
	for range streams {
		streamArgs = append(streamArgs, ">")
	}

	results, err := w.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    w.cfg.ConsumerGroup,
		Consumer: w.cfg.ConsumerName,
		Streams:  streamArgs,
		Count:    w.cfg.ReadCount,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	for _, result := range results {
		if len(result.Messages) == 0 {
			continue
		}
		return &streamMessage{Stream: result.Stream, Msg: result.Messages[0]}, nil
	}

	return nil, nil
}

func (w *Worker) processMessage(ctx context.Context, event streamMessage) error {
	scanRunID := valueAsString(event.Msg.Values["scan_run_id"])
	if scanRunID == "" {
		return w.ack(ctx, event.Stream, event.Msg.ID)
	}

	attempt, maxAttempts, err := w.markRunRunning(ctx, scanRunID)
	if err != nil {
		return err
	}

	if attempt > maxAttempts {
		if err := w.markMaxAttemptsExceeded(ctx, scanRunID, "max_attempts_exceeded", map[string]any{"source": "airflow-worker"}); err != nil {
			return err
		}

		return w.ack(ctx, event.Stream, event.Msg.ID)
	}

	payload := map[string]any{
		"scan_run_id":       scanRunID,
		"redis_message_id":  event.Msg.ID,
		"recurring_scan_id": valueAsString(event.Msg.Values["recurring_scan_id"]),
		"asset_id":          valueAsString(event.Msg.Values["asset_id"]),
		"asset_type":        valueAsString(event.Msg.Values["asset_type"]),
		"priority":          valueAsString(event.Msg.Values["priority"]),
	}

	dagRunID, err := w.triggerDAG(ctx, scanRunID, attempt, payload)
	if err != nil {
		return w.handleExecutionFailure(ctx, event, scanRunID, attempt, maxAttempts, "trigger_failed", err)
	}

	if err := w.attachDagRunID(ctx, scanRunID, dagRunID); err != nil {
		return err
	}

	state, err := w.waitForDAGCompletion(ctx, dagRunID)
	if err != nil {
		return w.handleExecutionFailure(ctx, event, scanRunID, attempt, maxAttempts, "monitor_failed", err)
	}

	if state != "success" {
		return w.handleExecutionFailure(ctx, event, scanRunID, attempt, maxAttempts, "dag_failed", fmt.Errorf("dag state: %s", state))
	}

	result, err := w.fetchDAGResult(ctx, dagRunID)
	if err != nil {
		w.logger.Warn("failed fetching airflow xcom result; using fallback static payload", "scan_run_id", scanRunID, "error", err)
		result = map[string]any{
			"status":  "ok",
			"payload": "static",
			"source":  "fallback",
			"dag_run": dagRunID,
		}
	}

	if err := w.markSucceeded(ctx, scanRunID, result); err != nil {
		return err
	}

	return w.ack(ctx, event.Stream, event.Msg.ID)
}

func (w *Worker) handleExecutionFailure(ctx context.Context, event streamMessage, scanRunID string, attempt, maxAttempts int, reason string, err error) error {
	details := map[string]any{
		"reason": reason,
		"error":  err.Error(),
	}

	if attempt >= maxAttempts {
		if err := w.markMaxAttemptsExceeded(ctx, scanRunID, reason, details); err != nil {
			return err
		}

		return w.ack(ctx, event.Stream, event.Msg.ID)
	}

	if err := w.requeueForRetry(ctx, event, scanRunID, reason, details); err != nil {
		return err
	}

	return w.ack(ctx, event.Stream, event.Msg.ID)
}

func (w *Worker) markRunRunning(ctx context.Context, scanRunID string) (attempt int, maxAttempts int, err error) {
	query := `
UPDATE recurring_scan_runs
SET status = 'running',
    attempt_count = attempt_count + 1,
    started_at = COALESCE(started_at, now()),
    updated_at = now()
WHERE id = $1::uuid
RETURNING attempt_count, max_attempts;
`

	if err := w.pg.QueryRow(ctx, query, scanRunID).Scan(&attempt, &maxAttempts); err != nil {
		return 0, 0, fmt.Errorf("mark run running: %w", err)
	}

	return attempt, maxAttempts, nil
}

func (w *Worker) attachDagRunID(ctx context.Context, scanRunID, dagRunID string) error {
	query := `
UPDATE recurring_scan_runs
SET airflow_dag_id = $2,
    airflow_dag_run_id = $3,
    updated_at = now()
WHERE id = $1::uuid;
`

	cmd, err := w.pg.Exec(ctx, query, scanRunID, w.cfg.AirflowDAGID, dagRunID)
	if err != nil {
		return fmt.Errorf("attach dag run id: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("attach dag run id: run %s not found", scanRunID)
	}

	return nil
}

func (w *Worker) markSucceeded(ctx context.Context, scanRunID string, result map[string]any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal success result: %w", err)
	}

	query := `
UPDATE recurring_scan_runs
SET status = 'succeeded',
    result = $2::jsonb,
    failure_reason = NULL,
    failure_details = NULL,
    finished_at = now(),
    updated_at = now()
WHERE id = $1::uuid;
`

	cmd, err := w.pg.Exec(ctx, query, scanRunID, raw)
	if err != nil {
		return fmt.Errorf("mark run succeeded: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("mark run succeeded: run %s not found", scanRunID)
	}

	return nil
}

func (w *Worker) requeueForRetry(ctx context.Context, event streamMessage, scanRunID, reason string, details map[string]any) error {
	rawDetails, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("marshal retry details: %w", err)
	}

	query := `
UPDATE recurring_scan_runs
SET status = 'enqueued',
    failure_reason = $2,
    failure_details = $3::jsonb,
    updated_at = now()
WHERE id = $1::uuid;
`

	cmd, err := w.pg.Exec(ctx, query, scanRunID, reason, rawDetails)
	if err != nil {
		return fmt.Errorf("mark run enqueued for retry: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("mark run enqueued for retry: run %s not found", scanRunID)
	}

	values := map[string]any{
		"scan_run_id":       scanRunID,
		"recurring_scan_id": valueAsString(event.Msg.Values["recurring_scan_id"]),
		"asset_id":          valueAsString(event.Msg.Values["asset_id"]),
		"asset_type":        valueAsString(event.Msg.Values["asset_type"]),
		"priority":          valueAsString(event.Msg.Values["priority"]),
		"scheduled_for":     valueAsString(event.Msg.Values["scheduled_for"]),
		"frequency":         valueAsString(event.Msg.Values["frequency"]),
	}

	if _, err := w.redis.XAdd(ctx, &redis.XAddArgs{Stream: event.Stream, Values: values}).Result(); err != nil {
		return fmt.Errorf("requeue failed message: %w", err)
	}

	return nil
}

func (w *Worker) markMaxAttemptsExceeded(ctx context.Context, scanRunID, reason string, details map[string]any) error {
	rawDetails, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("marshal max attempts details: %w", err)
	}

	query := `
UPDATE recurring_scan_runs
SET status = 'max_attempts_exceeded',
    failure_reason = $2,
    failure_details = $3::jsonb,
    finished_at = now(),
    updated_at = now()
WHERE id = $1::uuid;
`

	cmd, err := w.pg.Exec(ctx, query, scanRunID, reason, rawDetails)
	if err != nil {
		return fmt.Errorf("mark max attempts exceeded: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("mark max attempts exceeded: run %s not found", scanRunID)
	}

	return nil
}

func (w *Worker) ack(ctx context.Context, stream, messageID string) error {
	if _, err := w.redis.XAck(ctx, stream, w.cfg.ConsumerGroup, messageID).Result(); err != nil {
		return fmt.Errorf("xack stream message: %w", err)
	}

	return nil
}

func valueAsString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%.0f", v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}
