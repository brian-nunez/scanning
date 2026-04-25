from __future__ import annotations

import time
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="scan_example",
    description="Example scan DAG: sleeps for 10 seconds then returns static JSON.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["scanning"],
) as dag:
    @task(task_id="run_scan")
    def run_scan() -> dict:
        time.sleep(10)
        return {
            "status": "ok",
            "source": "scan_example",
            "payload": {
                "message": "static payload",
                "version": 1,
            },
        }

    run_scan()
