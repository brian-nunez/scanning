# Scanning Worker Sandbox

This workspace contains two Go worker apps based on your template:

- `scheduler-worker`: claims due recurring scans from Postgres and enqueues jobs into Redis Streams.
- `airflow-worker`: consumes Redis Stream jobs, triggers Airflow DAG runs, monitors completion, and writes final status/results back to Postgres.

## Services

Top-level `docker-compose.yml` starts:

- Postgres 18 (`postgres`)
- Redis (`redis`)
- RedisInsight (`redisinsight`)
- pgAdmin (`pgadmin`)
- Airflow (`airflow-init`, `airflow-webserver`, `airflow-scheduler`)
- Go Scheduler Worker (`scheduler-worker`)
- Go Airflow Worker (`airflow-worker`)

## Start

```bash
docker compose up --build
```

Airflow UI/API:

- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

RedisInsight:

- URL: `http://localhost:5540`
- Default connection is preloaded as `scanning-redis` (`redis:6379`, DB `0`).

pgAdmin:

- URL: `http://localhost:5050`
- Email: `admin@local.dev`
- Password: `admin`
- Default servers are preloaded:
  - `scanning-db` (`postgres:5432`, user `scanning`, db `scanning`)
  - `airflow-db` (`postgres:5432`, user `airflow`, db `airflow`)

## Notes

- Postgres is the source of truth (`recurring_scans`, `recurring_scan_runs`).
- Redis Streams is the delivery buffer (`scan_jobs:urgent`, `scan_jobs:normal`).
- Airflow executes the DAG (`scan_example`) with a 10-second sleep and static JSON result.
- `XACK` is done only after final Postgres update (succeeded, retry enqueue, or max attempts exceeded).
- Both worker implementations expose decoupled `Start(ctx)` / `Stop(ctx)` methods in `internal/worker`, so the worker logic can be moved to another repo with minimal integration work.
