from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class MinIOConf:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket: str = os.getenv("MINIO_BUCKET", "raw")
    secure: bool = os.getenv("MINIO_SECURE", "0") == "1"


@dataclass
class App:
    env: str = os.getenv("APP_ENV", "dev")


CFG = App()
MINIO = MinIOConf()
