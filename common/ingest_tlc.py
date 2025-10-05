from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import TLC_CFG
from .io_s3 import put_bytes
from .logging import get_logger
from .schemas import REQUIRED_TLC_MIN

log = get_logger()


@dataclass
class IngestStats:
    rows: int
    bytes: int
    sha256: str


def _url_for(month: str) -> str:
    yyyy, mm = month.split("-")
    return f"https://{TLC_CFG.host}/trip-data/yellow_tripdata_{yyyy}-{mm}.parquet"


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _validate_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_TLC_MIN if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _arrow_table(df: pd.DataFrame) -> pa.Table:
    # Keep raw types, but ensure timestamps are datetime64[ns]
    for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return pa.Table.from_pandas(df, preserve_index=False)


def bronze_key(month: str, sha: str) -> str:
    yyyy, mm = month.split("-")
    return f"bronze/trips/year={yyyy}/month={mm}/yellow_{yyyy}-{mm}_{sha[:12]}.parquet"


def manifest_key(month: str) -> str:
    return f"bronze/trips/_manifests/{month}.json"


def build_manifest(month: str, url: str, stats: IngestStats) -> dict[str, object]:
    return {
        "month": month,
        "source_url": url,
        "sha256": stats.sha256,
        "rows": stats.rows,
        "bytes": stats.bytes,
        "ingested_at": datetime.utcnow().isoformat(),
        "version": 1,
    }


def ingest_month(month: str) -> tuple[str, dict[str, object]]:
    url = _url_for(month)
    log.info("download_start", month=month, url=url)
    import requests  # type: ignore

    r = requests.get(url, timeout=120)
    r.raise_for_status()
    raw_bytes = r.content
    sha = _sha256(raw_bytes)

    # Load with pyarrow to DataFrame for column validation; don’t mutate values
    table = pq.read_table(io.BytesIO(raw_bytes))
    df = table.to_pandas(types_mapper=dict)  # Leave raw-ish types
    _validate_columns(df)

    # Re-write to Parquet to ensure uniform encoding & stats
    atable = _arrow_table(df)
    sink = io.BytesIO()
    pq.write_table(atable, sink, compression="snappy")
    data_bytes = sink.getvalue()

    stats = IngestStats(rows=df.shape[0], bytes=len(data_bytes), sha256=sha)
    key_parquet = bronze_key(month, sha)
    put_bytes(key_parquet, data_bytes, content_type="application/octet-stream")

    manifest = build_manifest(month, url, stats)
    put_bytes(manifest_key(month), json.dumps(manifest, indent=2).encode("utf-8"))

    log.info("ingest_ok", month=month, rows=stats.rows, sha=sha[:12])
    return key_parquet, manifest
