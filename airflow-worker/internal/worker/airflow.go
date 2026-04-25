package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type dagRunResponse struct {
	DagRunID string `json:"dag_run_id"`
	State    string `json:"state"`
}

type xcomEntryResponse struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func (w *Worker) triggerDAG(ctx context.Context, scanRunID string, attempt int, payload map[string]any) (string, error) {
	dagRunID := fmt.Sprintf("scan_%s_attempt_%d_%d", strings.ReplaceAll(scanRunID, "-", ""), attempt, time.Now().Unix())

	requestBody := map[string]any{
		"dag_run_id": dagRunID,
		"conf":       payload,
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("marshal airflow trigger payload: %w", err)
	}

	endpoint := fmt.Sprintf("%s/api/v1/dags/%s/dagRuns", strings.TrimSuffix(w.cfg.AirflowBaseURL, "/"), url.PathEscape(w.cfg.AirflowDAGID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create airflow trigger request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(w.cfg.AirflowUsername, w.cfg.AirflowPassword)

	res, err := w.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("execute airflow trigger request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 8*1024))
		return "", fmt.Errorf("airflow trigger returned %d: %s", res.StatusCode, string(raw))
	}

	return dagRunID, nil
}

func (w *Worker) waitForDAGCompletion(ctx context.Context, dagRunID string) (string, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, w.cfg.AirflowRunTimeout)
	defer cancel()

	ticker := time.NewTicker(w.cfg.AirflowPollInterval)
	defer ticker.Stop()

	for {
		state, err := w.fetchDAGRunState(deadlineCtx, dagRunID)
		if err != nil {
			return "", err
		}

		switch strings.ToLower(state) {
		case "success", "failed":
			return strings.ToLower(state), nil
		}

		select {
		case <-deadlineCtx.Done():
			return "", fmt.Errorf("airflow dag run timeout: %w", deadlineCtx.Err())
		case <-ticker.C:
		}
	}
}

func (w *Worker) fetchDAGRunState(ctx context.Context, dagRunID string) (string, error) {
	endpoint := fmt.Sprintf("%s/api/v1/dags/%s/dagRuns/%s",
		strings.TrimSuffix(w.cfg.AirflowBaseURL, "/"),
		url.PathEscape(w.cfg.AirflowDAGID),
		url.PathEscape(dagRunID),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("create airflow dag run state request: %w", err)
	}
	req.SetBasicAuth(w.cfg.AirflowUsername, w.cfg.AirflowPassword)

	res, err := w.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("execute airflow dag run state request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 8*1024))
		return "", fmt.Errorf("airflow state request returned %d: %s", res.StatusCode, string(raw))
	}

	var payload dagRunResponse
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode airflow state response: %w", err)
	}

	return payload.State, nil
}

func (w *Worker) fetchDAGResult(ctx context.Context, dagRunID string) (map[string]any, error) {
	endpoint := fmt.Sprintf("%s/api/v1/dags/%s/dagRuns/%s/taskInstances/%s/xcomEntries/return_value",
		strings.TrimSuffix(w.cfg.AirflowBaseURL, "/"),
		url.PathEscape(w.cfg.AirflowDAGID),
		url.PathEscape(dagRunID),
		url.PathEscape(w.cfg.AirflowTaskID),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create airflow xcom request: %w", err)
	}
	req.SetBasicAuth(w.cfg.AirflowUsername, w.cfg.AirflowPassword)

	res, err := w.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute airflow xcom request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 8*1024))
		return nil, fmt.Errorf("airflow xcom request returned %d: %s", res.StatusCode, string(raw))
	}

	var payload xcomEntryResponse
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode airflow xcom response: %w", err)
	}

	switch v := payload.Value.(type) {
	case map[string]any:
		return v, nil
	case string:
		var decoded map[string]any
		if err := json.Unmarshal([]byte(v), &decoded); err == nil {
			return decoded, nil
		}

		return map[string]any{"value": v}, nil
	default:
		raw, _ := json.Marshal(v)
		return map[string]any{"value": string(raw)}, nil
	}
}
