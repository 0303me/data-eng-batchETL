from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task

from common.io_s3 import put_bytes
from common.logging import get_logger

log = get_logger()


def _bronze_key(ds: str) -> str:
    return f"bronze/trips/year={ds[:4]}/month={ds[5:7]}/sample.parquet"


with DAG(
    dag_id="ingest_tlc_raw_daily",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    default_args={"owner": "data"},
    tags=["bronze", "tlc"],
):

    @task
    def write_sample(**context):  # type: ignore[no-untyped-def]
        ds = context["ds"]
        df = pd.DataFrame(
            {"vendor_id": [1], "pickup_ts": [ds + "T02:00:00"], "fare_amount": [12.5]}
        )
        parquet = df.to_parquet(index=False)
        put_bytes(_bronze_key(ds), parquet, content_type="application/octet-stream")
        log.info("bronze_parquet_written", ds=ds)

    @task
    def write_manifest(**context):  # type: ignore[no-untyped-def]
        ds = context["ds"]
        manifest = {"run_date": ds, "partitions": [ds[:7]]}
        put_bytes(f"bronze/trips/_manifests/{ds}.json", json.dumps(manifest).encode())
        log.info("manifest_written", ds=ds)

    write_sample() >> write_manifest()
