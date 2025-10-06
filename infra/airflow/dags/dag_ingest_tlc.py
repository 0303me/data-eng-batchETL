from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from botocore.exceptions import ClientError

from common.config import TLC_CFG
from common.ingest_tlc import ingest_month, manifest_key
from common.io_s3 import s3
from common.logging import get_logger

log = get_logger()


def _months() -> list[str]:
    return [m.strip() for m in TLC_CFG.months.split(",") if m.strip()]


with DAG(
    dag_id="ingest_tlc_raw_monthly",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    default_args={"owner": "data"},
    tags=["bronze", "tlc"],
):

    @task()  # type: ignore[misc]
    def plan_months() -> list[str]:
        ms = _months()
        log.info("plan_months", months=ms)
        return ms

    @task()  # type: ignore[misc]
    def maybe_ingest(month: str, force: bool = False) -> dict[str, object]:
        # Skip if manifest exists unless force
        if not force:
            try:
                s3.head_object(
                    Bucket=os.environ.get("MINIO_BUCKET", "raw"), Key=manifest_key(month)
                )
                log.info("skip_existing", month=month)
                return {"month": month, "skipped": True, "reason": "manifest_exists"}
            except ClientError:
                pass
        key, manifest = ingest_month(month)
        if key is None:
            # Month not yet published
            return manifest
        return {"month": month, "skipped": False, "key": key, "manifest": manifest}

    months = plan_months()
    _ = maybe_ingest.expand(month=months)
